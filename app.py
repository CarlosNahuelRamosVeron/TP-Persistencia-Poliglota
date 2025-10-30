# app.py — Orquestador CLI de reporte mensual por ciudad
import argparse
import calendar
from datetime import datetime
import os

import cassandra_part as cas
import mongo_part as mon
import redis_module as rds   # toleramos que no esté disponible en runtime

PROCESS_ID   = "proc_temp_max_min"
PROCESS_NAME = "Max/Min por ciudad"
PROCESS_DESC = "Informe de extremos"
PROCESS_PRICE = 10.0

def run_monthly_city_report(user_id: str, country: str, city: str, year: int, month: int):
    # --- Rango YYYY-MM-DD ---
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to   = f"{year}-{month:02d}-{last_day:02d}"

    # --- 1) Request en Mongo ---
    mon.bootstrap_indexes()
    req_id = mon.request_process(
        user_id, PROCESS_ID,
        {"country": country, "city": city, "from": date_from, "to": date_to, "granularity": "monthly"}
    )

    # --- 2) Rollups desde Cassandra y consolidación ---
    sess = cas.bootstrap_schema()
    results = []
    for day in range(1, last_day + 1):
        yyyymmdd = year * 10000 + month * 100 + day
        cas.rollup_daily_for_city(sess, country, city, yyyymmdd)
        rows = sess.execute("""
           SELECT temp_min, temp_max, hum_min, hum_max, temp_avg, hum_avg
           FROM daily_city_stats
           WHERE country=%s AND city=%s AND yyyymmdd=%s
        """, (country, city, yyyymmdd))
        results.extend(rows)

    # --- 3) Reducción mensual ---
    if results:
        tmax = max([float(x.temp_max) for x in results if x.temp_max is not None], default=None)
        tmin = min([float(x.temp_min) for x in results if x.temp_min is not None], default=None)
        exec_id = mon.record_execution(req_id, ok=True, resultLocation="s3://fake/report.pdf", meteredUnits=len(results))

        # --- 4) Intentar uso en Redis (no crítico) ---
        try:
            rds.incr_usage(user_id, f"{year}{month:02d}", len(results))
        except Exception:
            pass  # no rompemos el flujo si Redis no responde

        return {
            "ok": True, "req_id": req_id, "exec_id": exec_id,
            "temp_max": tmax, "temp_min": tmin, "from": date_from, "to": date_to,
            "days": len(results)
        }
    else:
        mon.record_execution(req_id, ok=False, notes="No hay datos en el rango")
        return {"ok": False, "req_id": req_id, "error": "sin datos", "from": date_from, "to": date_to}

def ensure_process_catalog():
    mon.define_process(
        PROCESS_ID, PROCESS_NAME, PROCESS_DESC, PROCESS_PRICE,
        paramsSpec={"country": "string", "city": "string", "from": "date", "to": "date",
                    "granularity": ["daily", "monthly"]}
    )

def build_parser():
    p = argparse.ArgumentParser(description="Runner de reporte mensual por ciudad")
    p.add_argument("--country", required=True, help="Código país (ej: AR)")
    p.add_argument("--city", required=True, help='Nombre de ciudad (ej: "Buenos Aires")')
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    # Usuario: o pasás el ID o pasás email+nombre para crearlo/reutilizarlo
    p.add_argument("--user-id", help="ID de usuario existente (usr_...)")
    p.add_argument("--user-email", help="Email para crear/asegurar usuario")
    p.add_argument("--user-name", default="Usuario CLI", help="Nombre del usuario (si se crea)")
    p.add_argument("--role", default="usuario", help="Rol del usuario nuevo (por defecto: usuario)")
    p.add_argument("--no-invoice", action="store_true", help="No emitir factura ni registrar pago")
    return p

def main():
    args = build_parser().parse_args()

    # Asegurar proceso en catálogo
    ensure_process_catalog()

    # Asegurar usuario
    if args.user_id:
        uid = args.user_id
    else:
        if not args.user_email:
            raise SystemExit("Debes pasar --user-id o --user-email")
        uid = mon.create_user(args.user_email, args.user_name, "pwd$cli", roles=[args.role])

    # Ejecutar reporte
    out = run_monthly_city_report(uid, args.country, args.city, args.year, args.month)

    if out.get("ok"):
        print("✅ Reporte OK:")
        print(f"  Usuario: {uid}")
        print(f"  Rango  : {out['from']} → {out['to']}  (días con datos: {out['days']})")
        print(f"  Temp   : min={out['temp_min']}  max={out['temp_max']}")
        print(f"  req_id : {out['req_id']}")
        print(f"  exec_id: {out['exec_id']}")

        if not args.no_invoice:
            # Intentar factura y pago
            try:
                inv_id, total = mon.issue_invoice(uid, [{
                    "processId": PROCESS_ID, "qty": 1, "unitPrice": PROCESS_PRICE, "amount": PROCESS_PRICE
                }])
                mon.register_payment(inv_id, total, "tarjeta")
                print(f"  Factura: {inv_id}  total=${total}  (pagada)")
            except Exception as e:
                print(f"  ⚠️ Facturación omitida por error: {e}")
    else:
        print(" Reporte fallido:")
        print(f"  Usuario: {uid}")
        print(f"  Rango  : {out['from']} → {out['to']}")
        print(f"  Motivo : {out.get('error','desconocido')}")
        # No facturamos si falló

if __name__ == "__main__":
    main()
