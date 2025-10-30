# menu.py — Menú interactivo para TP Persistencia Políglota
import sys
import traceback
from datetime import datetime, timezone, timedelta
import calendar

# Importa tus módulos locales
import cassandra_part as cas
import mongo_part as mon
import redis_module as rds
import neo4j_module as neo

BA_TZ = timezone(timedelta(hours=-3))

PROCESS_ID   = "proc_temp_max_min"
PROCESS_NAME = "Max/Min por ciudad"
PROCESS_DESC = "Informe de extremos"
PROCESS_PRICE= 10.0

# ---------------------------
# Helpers de I/O
# ---------------------------
def ask(prompt, default=None, cast=str):
    txt = input(f"{prompt}" + (f" [{default}]" if default is not None else "") + ": ").strip()
    if not txt and default is not None:
        return default
    if cast is int:
        return int(txt)
    if cast is float:
        return float(txt)
    return txt

def pause():
    input("\nPresioná Enter para continuar...")

def now_ba():
    return datetime.now(BA_TZ)

def print_ok(msg):
    print(f" {msg}")

def print_err(msg):
    print(f" {msg}")

def guard(fn, success_msg=None):
    try:
        out = fn()
        if success_msg:
            print_ok(success_msg)
        return out
    except Exception as e:
        print_err(f"Error: {e}")
        traceback.print_exc(limit=1)
        return None

# ---------------------------
# Operaciones compuestas
# ---------------------------
def ensure_process_catalog():
    mon.define_process(
        PROCESS_ID, PROCESS_NAME, PROCESS_DESC, PROCESS_PRICE,
        paramsSpec={"country":"string","city":"string","from":"date","to":"date","granularity":["daily","monthly"]}
    )

def run_monthly_city_report(user_id: str, country: str, city: str, year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to   = f"{year}-{month:02d}-{last_day:02d}"

    mon.bootstrap_indexes()
    ensure_process_catalog()

    req_id = mon.request_process(
        user_id, PROCESS_ID,
        {"country": country, "city": city, "from": date_from, "to": date_to, "granularity": "monthly"}
    )

    sess = cas.bootstrap_schema()
    results = []
    for day in range(1, last_day + 1):
        yyyymmdd = year*10000 + month*100 + day
        cas.rollup_daily_for_city(sess, country, city, yyyymmdd)
        rows = sess.execute("""
           SELECT temp_min, temp_max, hum_min, hum_max, temp_avg, hum_avg
           FROM daily_city_stats
           WHERE country=%s AND city=%s AND yyyymmdd=%s
        """, (country, city, yyyymmdd))
        results.extend(rows)

    if results:
        tmax = max([float(x.temp_max) for x in results if x.temp_max is not None], default=None)
        tmin = min([float(x.temp_min) for x in results if x.temp_min is not None], default=None)
        exec_id = mon.record_execution(req_id, ok=True, resultLocation="s3://fake/report.pdf", meteredUnits=len(results))
        try:
            rds.incr_usage(user_id, f"{year}{month:02d}", len(results))
        except Exception:
            pass
        return {"ok": True, "req_id": req_id, "exec_id": exec_id, "temp_max": tmax, "temp_min": tmin,
                "from": date_from, "to": date_to, "days": len(results)}
    else:
        mon.record_execution(req_id, ok=False, notes="No hay datos en el rango")
        return {"ok": False, "req_id": req_id, "error":"sin datos", "from": date_from, "to": date_to}

# ---------------------------
# Menú de opciones
# ---------------------------
def opt_1_bootstrap_all():
    guard(lambda: cas.bootstrap_schema(), "Cassandra OK (keyspace/tablas)")
    guard(mon.bootstrap_indexes, "MongoDB OK (índices)")
    guard(neo.bootstrap_model, "Neo4j OK (constraints)")
    print_ok("Entorno inicial preparado")

def opt_2_cassandra_demo():
    s = guard(cas.bootstrap_schema)
    if not s: return
    sensor_id = "S-AR-0001"
    guard(lambda: cas.upsert_sensor(s, {
        "sensor_id": sensor_id, "name": "Obelisco", "type":"mixto",
        "lat": -34.6037, "lon": -58.3816, "city":"Buenos Aires", "country":"AR",
        "status": "activo", "started_at": datetime.utcnow()
    }), "Sensor asegurado")
    when_local = ask("Fecha/hora (BA) YYYY-MM-DD HH:MM[:SS]", now_ba().strftime("%Y-%m-%d %H:%M:%S"))
    temp = ask("Temperatura °C", "22.8", float)
    hum  = ask("Humedad (0..1 o 0..100)", "55", float)

    # Usamos parser del módulo para convertir a UTC
    when_utc = cas.__dict__.get("parse_local_ba")(when_local) if "parse_local_ba" in cas.__dict__ else datetime.utcnow()
    hum_norm = cas.normalize_humidity(hum)
    guard(lambda: cas.insert_measurement(s, sensor_id, when_utc, temp, hum_norm, "Buenos Aires", "AR"),
          "Medición insertada")
    # Query hoy
    start_ba = now_ba().replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_ba.astimezone(timezone.utc).replace(tzinfo=None)
    rows = list(cas.query_sensor_range(s, sensor_id, start_utc, datetime.utcnow()))
    cas.print_measurements(rows, use_ba_time=True)

def opt_3_mongo_create_user_and_session():
    email = ask("Email", "demo@uade.com")
    name  = ask("Nombre", "Usuario Demo")
    pwd   = ask("Password (hash demo)", "pwd$demo")
    role  = ask("Rol", "usuario")
    guard(mon.bootstrap_indexes)
    uid = guard(lambda: mon.create_user(email, name, pwd, roles=[role]))
    if uid:
        sid = guard(lambda: mon.open_session(uid, role))
        print_ok(f"UserID={uid}  SessionID={sid}")

def opt_4_neo_bootstrap_seed_admin():
    guard(neo.bootstrap_model, "Constraints OK")
    guard(neo.seed_minimal, "Seeds OK (roles/procesos)")
    guard(neo.grant_all_to_admin, "Rol admin con acceso total")
    uid = ask("Crear usuario admin (id)", "usr_admin")
    guard(lambda: neo.create_user_with_role(uid, "admin"), f"Usuario {uid} asignado a admin")
    rows = neo.processes_user_can_run(uid)
    neo.print_processes(rows)

def opt_5_run_report_and_invoice():
    # Usuario (idempotente)
    email = ask("Email usuario", "nahuel@ejemplo.com")
    name  = ask("Nombre", "Usuario Prueba")
    uid   = guard(lambda: mon.create_user(email, name, "pwd$cli", roles=["usuario"]))
    if not uid: return
    # Parámetros
    country = ask("País (ISO2)", "AR")
    city    = ask("Ciudad", "Buenos Aires")
    year    = ask("Año", now_ba().year, int)
    month   = ask("Mes (1-12)", now_ba().month, int)

    ensure_process_catalog()
    out = run_monthly_city_report(uid, country, city, year, month)
    if out.get("ok"):
        print_ok("Reporte OK")
        print(f"Rango: {out['from']} → {out['to']}  días={out['days']}")
        print(f"Temp: min={out['temp_min']}  max={out['temp_max']}")
        # Facturar
        do_invoice = ask("¿Emitir factura? (s/n)", "s")
        if do_invoice.lower().startswith("s"):
            inv_id, total = mon.issue_invoice(uid, [{
                "processId": PROCESS_ID, "qty": 1, "unitPrice": PROCESS_PRICE, "amount": PROCESS_PRICE
            }])
            mon.register_payment(inv_id, total, "tarjeta")
            print_ok(f"Factura {inv_id} total=${total} (pagada)")
    else:
        print_err(f"Reporte fallido: {out.get('error')}")

def opt_6_redis_alert_flow():
    aid = ask("ID de alerta", "alr_0001")
    sensor = ask("Sensor", "S-AR-0001")
    desc = ask("Descripción", "Temperatura fuera de rango")
    when = ask("Fecha/hora (BA) YYYY-MM-DD HH:MM[:SS]", now_ba().strftime("%Y-%m-%d %H:%M:%S"))
    ts = rds.__dict__.get("parse_when_to_epoch")(when) if "parse_when_to_epoch" in rds.__dict__ else int(datetime.utcnow().timestamp())
    guard(lambda: rds.push_alert(aid, ts, {"type":"sensor","sensor_id":sensor,"desc":desc,"ts":ts}),
          f"Alerta {aid} creada")
    # Listar activas (simple)
    try:
        items = rds.r.zrevrange("alerts:active", 0, 19, withscores=True)
        print("\nAlertas activas:")
        for key, score in items:
            h = rds.r.hgetall(key)
            print(f"- {key}  ts={h.get('ts')}  state={h.get('state')}  sensor={h.get('sensor_id')}  desc={h.get('desc')}")
    except Exception as e:
        print_err(f"No se pudo listar alertas: {e}")

def opt_7_neo_user_perms():
    uid = ask("User ID", "usr_0001")
    rows = neo.processes_user_can_run(uid)
    neo.print_processes(rows)

def opt_8_status():
    print("\n== Estado ==")
    # Redis
    try:
        pong = rds.r.ping()
        print_ok(f"Redis conectado: {pong}")
    except Exception as e:
        print_err(f"Redis no disponible: {e}")
    # Mongo
    try:
        mon.db.list_collection_names()
        print_ok("Mongo conectado")
    except Exception as e:
        print_err(f"Mongo no disponible: {e}")
    # Cassandra
    try:
        s = cas.bootstrap_schema()
        print_ok("Cassandra OK (keyspace listo)")
    except Exception as e:
        print_err(f"Cassandra no disponible: {e}")
    # Neo4j
    try:
        neo.bootstrap_model()
        print_ok("Neo4j OK (constraints verificados)")
    except Exception as e:
        print_err(f"Neo4j no disponible: {e}")

def opt_9_exit():
    print("\n¡Hasta la próxima!")
    sys.exit(0)

OPTIONS = [
    ("Preparar entorno (Cassandra/Mongo/Neo4j)", opt_1_bootstrap_all),
    ("Cassandra: demo (insertar y consultar)",   opt_2_cassandra_demo),
    ("Mongo: crear usuario + sesión",            opt_3_mongo_create_user_and_session),
    ("Neo4j: bootstrap + seeds + admin",         opt_4_neo_bootstrap_seed_admin),
    ("Reporte mensual + factura",                 opt_5_run_report_and_invoice),
    ("Redis: crear y ver alerta",                 opt_6_redis_alert_flow),
    ("Neo4j: listar procesos por usuario",        opt_7_neo_user_perms),
    ("Ver estado de servicios",                   opt_8_status),
    ("Salir",                                     opt_9_exit),
]

def main():
    while True:
        print("\n==============================")
        print("  TP – Persistencia Políglota ")
        print("=========== MENÚ =============")
        for i, (label, _) in enumerate(OPTIONS, start=1):
            print(f"{i}. {label}")
        try:
            choice = int(ask("Elegí una opción", 1, int))
        except ValueError:
            print_err("Opción inválida")
            continue

        if 1 <= choice <= len(OPTIONS):
            _, fn = OPTIONS[choice-1]
            fn()
            pause()
        else:
            print_err("Opción fuera de rango")

if __name__ == "__main__":
    main()