
from datetime import datetime, timedelta
import os


import cassandra_part as cas
import mongo_part as mon
import redis_module as rds

def run_monthly_city_report(user_id: str, country: str, city: str, year: int, month: int):
    # 1) Crear request en Mongo
    mon.bootstrap_indexes()
    req_id = mon.request_process(
        user_id, "proc_temp_max_min",
        {"country":country, "city":city,
         "from": f"{year}-{month:02d}-01",
         "to":   f"{year}-{month:02d}-28", # simplificación
         "granularity":"monthly"}
    )

    # 2) Leer rollups diarios desde Cassandra y consolidar
    sess = cas.bootstrap_schema()
    # si el mes tiene 30/31 o feb, ajustalo según calendario
    results = []
    for day in range(1, 29):  # simple
        yyyymmdd = year*10000 + month*100 + day
        cas.rollup_daily_for_city(sess, country, city, yyyymmdd)
        rows = sess.execute("""
           SELECT temp_min,temp_max,hum_min,hum_max,temp_avg,hum_avg
           FROM daily_city_stats WHERE country=%s AND city=%s AND yyyymmdd=%s
        """, (country, city, yyyymmdd))
        for r in rows: results.append(r)

    # 3) Reducir a resumen mensual (en la app)
    if results:
        tmax = max([float(x.temp_max) for x in results if x.temp_max is not None], default=None)
        tmin = min([float(x.temp_min) for x in results if x.temp_min is not None], default=None)
        # 4) Guardar ejecución OK y “meteredUnits” (ej. cantidad de lecturas procesadas)
        exec_id = mon.record_execution(req_id, ok=True, resultLocation="s3://fake/report.pdf", meteredUnits=len(results))
        # 5) Contabilizar uso en Redis para pricing variable
        rds.incr_usage(user_id, f"{year}{month:02d}", len(results))
        return {"req_id": req_id, "exec_id": exec_id, "temp_max": tmax, "temp_min": tmin}
    else:
        mon.record_execution(req_id, ok=False, notes="No hay datos")
        return {"req_id": req_id, "error":"sin datos"}

if __name__ == "__main__":
    # DEMO: crear usuario, proceso y correr un informe
    uid = mon.create_user("demo@ejemplo.com","Usuario Demo","argon2id$demo", roles=["usuario"])
    mon.define_process("proc_temp_max_min","Max/Min por ciudad","Informe",10.0,
                       paramsSpec={"country":"string","city":"string","from":"date","to":"date","granularity":["daily","monthly"]})
    out = run_monthly_city_report(uid, "AR", "Buenos Aires", 2025, 10)
    # facturar ítem simple
    if "exec_id" in out:
        inv_id, total = mon.issue_invoice(uid, [{"processId":"proc_temp_max_min","qty":1,"unitPrice":10.0,"amount":10.0}])
        print("Reporte OK:", out, "Factura:", inv_id, total)
    else:
        print("Reporte fallido:", out)
