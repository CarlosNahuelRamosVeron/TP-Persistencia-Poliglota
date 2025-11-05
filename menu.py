import sys
import traceback
from datetime import datetime, timezone, timedelta
import calendar
import subprocess, time, os

import cassandra_part as cas
import mongo_part as mon
import redis_module as rds
import neo4j_module as neo
import alerts_part as al
import app
import math, random

from users_part import (
    bootstrap_indexes as user_bootstrap,
    seed_basic_roles,
    create_user,
    assign_role,
    get_user_by_email,
)
from auth_part import login, logout, get_active_sessions, session_has_permission
from messaging_part import (
    create_group,
    send_private_message,
    send_group_message,
    list_inbox,
    mark_as_read,
)

# --------------------------------------------------------------------
# Config/constantes
# --------------------------------------------------------------------
BA_TZ = timezone(timedelta(hours=-3))

PROCESS_ID    = "proc_temp_max_min"
PROCESS_NAME  = "Max/Min por ciudad"
PROCESS_DESC  = "Informe de extremos"
PROCESS_PRICE = 10.0  # (queda para catálogo; NO se factura)

# --------------------------------------------------------------------
# Helpers generales
# --------------------------------------------------------------------
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
    print(f"{msg}")

def print_err(msg):
    print(f"{msg}")

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

def _user_by_email(email: str):
    u = get_user_by_email(email)
    if not u:
        raise ValueError("Usuario no encontrado")
    return u

def _require_perm(session_id: str, perm: str):
    if not session_has_permission(session_id, perm):
        raise PermissionError(f"Permiso requerido: {perm}")

# --------------------------------------------------------------------
# Catálogo de procesos (Mongo)
# --------------------------------------------------------------------
def ensure_process_catalog():
    mon.define_process(
        PROCESS_ID,
        PROCESS_NAME,
        PROCESS_DESC,
        PROCESS_PRICE,
        paramsSpec={"country": "string", "city": "string", "from": "date", "to": "date", "granularity": ["daily", "monthly"]},
    )

# --------------------------------------------------------------------
# Reporte mensual (extremos) - Cassandra + Mongo + Redis
# (Se deja acá para que el menú sea autocontenido. Si preferís, movelo a app.py)
# --------------------------------------------------------------------
def run_monthly_city_report(user_id: str, country: str, city: str, year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to   = f"{year}-{month:02d}-{last_day:02d}"

    mon.bootstrap_indexes()
    ensure_process_catalog()

    req_id = mon.request_process(
        user_id,
        PROCESS_ID,
        {"country": country, "city": city, "from": date_from, "to": date_to, "granularity": "monthly"},
    )

    sess = cas.bootstrap_schema()
    results = []
    for day in range(1, last_day + 1):
        yyyymmdd = year * 10000 + month * 100 + day
        cas.rollup_daily_for_city(sess, country, city, yyyymmdd)
        rows = sess.execute(
            """
            SELECT temp_min, temp_max, hum_min, hum_max, temp_avg, hum_avg
            FROM daily_city_stats
            WHERE country=%s AND city=%s AND yyyymmdd=%s
            """,
            (country, city, yyyymmdd),
        )
        results.extend(rows)

    if results:
        tmax = max([float(x.temp_max) for x in results if x.temp_max is not None], default=None)
        tmin = min([float(x.temp_min) for x in results if x.temp_min is not None], default=None)
        exec_id = mon.record_execution(
            req_id,
            ok=True,
            resultLocation="s3://fake/report.pdf",  # placeholder
            meteredUnits=len(results),
        )
        try:
            rds.incr_usage(user_id, f"{year}{month:02d}", len(results))
        except Exception:
            pass
        return {
            "ok": True,
            "req_id": req_id,
            "exec_id": exec_id,
            "temp_max": tmax,
            "temp_min": tmin,
            "from": date_from,
            "to": date_to,
            "days": len(results),
        }
    else:
        mon.record_execution(req_id, ok=False, notes="No hay datos en el rango")
        return {"ok": False, "req_id": req_id, "error": "sin datos", "from": date_from, "to": date_to}

# --------------------------------------------------------------------
# Opciones originales (infra/reportes)
# --------------------------------------------------------------------
def run_cmd(cmd: str):
    try:
        out = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        return out.decode("utf-8").strip()
    except subprocess.CalledProcessError as e:
        return e.output.decode("utf-8").strip()

def wait_port(host: str, port: int, timeout=30):
    import socket
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except Exception:
            time.sleep(1)
    return False

def opt_1_bootstrap_all():
    print("\n== 🔧 Iniciando entorno de bases de datos ==")

    # Si hay docker-compose
    if os.path.exists("docker-compose.yml") or os.path.exists("compose.yaml"):
        print_ok("Docker Compose detectado → levantando servicios…")
        print(run_cmd("docker compose up -d"))
        time.sleep(5)
    else:
        print("No se detectó docker-compose.yml, intento levantar motores locales o contenedores existentes.")

        # Mongo
        if not wait_port("127.0.0.1", 27017, 3):
            print("→ MongoDB no responde; intentando docker…")
            run_cmd("docker start mongo 2>/dev/null || true")

        # Redis
        if not wait_port("127.0.0.1", 6379, 3):
            print("→ Redis no responde; intentando docker…")
            run_cmd("docker start redis 2>/dev/null || true")

        # Cassandra
        if not wait_port("127.0.0.1", 9042, 3):
            print("→ Cassandra no responde; intentando docker…")
            run_cmd("docker start cassandra 2>/dev/null || true")

        # Neo4j
        if not wait_port("127.0.0.1", 7687, 3):
            print("→ Neo4j no responde; intentando docker…")
            run_cmd("docker start neo4j 2>/dev/null || true")

        print("Esperando que los motores se inicialicen (10s)…")
        time.sleep(10)

    # Pruebas de conexión y bootstrap
    try:
        guard(lambda: cas.bootstrap_schema(), "Cassandra OK (keyspace/tablas)")
    except Exception as e:
        print_err(f"Cassandra no disponible: {e}")

    try:
        guard(mon.bootstrap_indexes, "MongoDB OK (índices)")
    except Exception as e:
        print_err(f"MongoDB no disponible: {e}")

    try:
        guard(neo.bootstrap_model, "Neo4j OK (constraints)")
    except Exception as e:
        print_err(f"Neo4j no disponible: {e}")

    try:
        guard(user_bootstrap, "Usuarios OK (índices)")
        guard(seed_basic_roles, "Roles base creados (usuario/tecnico/administrador)")
    except Exception as e:
        print_err(f"Usuarios/Roles no disponibles: {e}")

    print_ok("Entorno inicial preparado")



def opt_2_cassandra():
    s = guard(cas.bootstrap_schema)
    if not s:
        return

    print("\n=== Cassandra: ===")

    # Parámetros básicos
    country = ask("País (ISO2)", "AR")
    city    = ask("Ciudad", "Buenos Aires")
    sensor_name = ask("Nombre del sensor", f"Sensor {city}")
    sensor_id   = ask("Sensor ID", f"S-{country}-{city[:3].upper()}-001")

    # Rango temporal e intervalo
    default_start = now_ba().replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
    start_local_s = ask("Fecha/hora inicial (BA) YYYY-MM-DD HH:MM", default_start)
    hours         = ask("Cantidad de horas a generar", "24", int)
    interval_min  = ask("Intervalo (minutos) entre mediciones", "60", int)

    # Parámetros de generación (temp/humedad)
    base_temp = ask("Temp base (°C)", "22.0", float)
    amp_temp  = ask("Amplitud temp (±°C)", "5.0", float)
    base_hum  = ask("Humedad base (0..1 o 0..100)", "60", float)
    amp_hum   = ask("Amplitud hum (± en mismas unidades)", "10", float)

    def parse_local_ba(s):
        parse_fn = cas.__dict__.get("parse_local_ba")
        if callable(parse_fn):
            return parse_fn(s)
        dt_ba = datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=BA_TZ)
        return dt_ba.astimezone(timezone.utc).replace(tzinfo=None)

    def to_utc_naive(dt_ba):
        return dt_ba.astimezone(timezone.utc).replace(tzinfo=None)
    guard(
        lambda: cas.upsert_sensor(
            s,
            {
                "sensor_id": sensor_id,
                "name": sensor_name,
                "type": "mixto",
                "city": city,
                "country": country,
                "status": "activo",
                "started_at": datetime.utcnow(),
            },
        ),
        f"Sensor asegurado: {sensor_id} / {city}, {country}",
    )

    # Generación e inserción
    start_utc = parse_local_ba(start_local_s)
    steps = max(1, (hours * 60) // max(1, interval_min))
    print(f"Insertando {steps} mediciones desde {start_local_s} (BA), cada {interval_min} min...")

    ok, fail = 0, 0
    for i in range(steps):
        #t_ba  = now_ba().replace(hour=0, minute=0, second=0, microsecond=0)  # no usamos esto para el cálculo
        t_utc = start_utc + timedelta(minutes=i * interval_min)

        phase = (i / max(1, steps-1)) * 2 * math.pi
        temp  = base_temp + math.sin(phase) * amp_temp + random.uniform(-0.5, 0.5)
        hum_v = base_hum  + math.cos(phase) * amp_hum  + random.uniform(-1.0, 1.0)

        try:
            hum_norm = cas.normalize_humidity(hum_v)
        except Exception:
            hum_norm = hum_v/100.0 if hum_v > 1.0 else hum_v
            hum_norm = max(0.0, min(1.0, hum_norm))

        try:
            cas.insert_measurement(s, sensor_id, t_utc, temp, hum_norm, city, country)
            ok += 1
        except Exception as e:
            fail += 1
            print(f"[WARN] No se pudo insertar medición #{i+1}: {e}")

    print(f"Listo. Insertadas OK: {ok} | Fallidas: {fail}")

    start_ba = datetime.strptime(start_local_s, "%Y-%m-%d %H:%M").replace(tzinfo=BA_TZ)
    end_ba   = start_ba + timedelta(hours=hours)
    q_start  = to_utc_naive(start_ba)
    q_end    = to_utc_naive(end_ba)

    try:
        rows = list(cas.query_sensor_range(s, sensor_id, q_start, q_end))
        print(f"Total recuperado para {sensor_id} en rango: {len(rows)}")
        if hasattr(cas, "print_measurements"):
            cas.print_measurements(rows[:min(20, len(rows))], use_ba_time=True)
        else:
            for r in rows[:min(10, len(rows))]:
                print(r)
    except Exception as e:
        print(f"[WARN] No se pudo listar rango insertado: {e}")


def opt_3_mongo_create_user_and_session():
    email = ask("Email", "demo@uade.com")
    name = ask("Nombre", "Usuario Demo")
    pwd = ask("Password", "pwd$demo")
    role = ask("Rol", "usuario")
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

def opt_5_run_report_extremes():
    email = ask("Email usuario", "nahuel@ejemplo.com")
    name = ask("Nombre", "Usuario Prueba")
    uid = guard(lambda: mon.create_user(email, name, "pwd$cli", roles=["usuario"]))
    if not uid:
        return

    country = ask("País (ISO2)", "AR")
    city = ask("Ciudad", "Buenos Aires")
    year = ask("Año", now_ba().year, int)
    month = ask("Mes (1-12)", now_ba().month, int)

    ensure_process_catalog()
    out = run_monthly_city_report(uid, country, city, year, month)
    if out.get("ok"):
        print_ok("Reporte OK")
        print(f"Rango: {out['from']} → {out['to']}  días={out['days']}")
        print(f"Temp: min={out['temp_min']}  max={out['temp_max']}")
    else:
        print_err(f"Reporte fallido: {out.get('error')}")

def opt_6_redis_alert_flow():
    aid = ask("ID de alerta", "alr_0001")
    sensor = ask("Sensor", "S-AR-0001")
    desc = ask("Descripción", "Temperatura fuera de rango")
    when = ask("Fecha/hora (BA) YYYY-MM-DD HH:MM[:SS]", now_ba().strftime("%Y-%m-%d %H:%M:%S"))
    ts = (
        rds.__dict__.get("parse_when_to_epoch")(when)
        if "parse_when_to_epoch" in rds.__dict__
        else int(datetime.utcnow().timestamp())
    )
    guard(
        lambda: rds.push_alert(aid, ts, {"type": "sensor", "sensor_id": sensor, "desc": desc, "ts": ts}),
        f"Alerta {aid} creada",
    )

    try:
        items = rds.r.zrevrange("alerts:active", 0, 19, withscores=True)
        print("\nAlertas activas:")
        for key, score in items:
            h = rds.r.hgetall(key)
            print(
                f"- {key}  ts={h.get('ts')}  state={h.get('state')}  sensor={h.get('sensor_id')}  desc={h.get('desc')}"
            )
    except Exception as e:
        print_err(f"No se pudo listar alertas: {e}")

def opt_7_neo_user_perms():
    uid = ask("User ID", "usr_0001")
    rows = neo.processes_user_can_run(uid)
    neo.print_processes(rows)

def opt_9_alertas_check():
    print("\n=== Control y Alertas ===")

    if hasattr(cas, "get_session"):
        s = cas.get_session()
    else:
        s = cas.bootstrap_schema()

    try:
        n_inactive = al.check_sensor_activity(s)
        n_temp = al.check_temperature_limits(s)
        n_hum = al.check_humidity_limits(s)

        n_inactive = n_inactive if isinstance(n_inactive, int) and n_inactive >= 0 else 0
        n_temp = n_temp if isinstance(n_temp, int) and n_temp >= 0 else 0
        n_hum = n_hum if isinstance(n_hum, int) and n_hum >= 0 else 0

        print("\n--- Resultados del chequeo ---")
        print(f"Alertas por inactividad generadas: {n_inactive}")
        print(f"Alertas por temperatura fuera de rango: {n_temp}")
        print(f"Alertas por humedad fuera de rango: {n_hum}")

    except Exception as e:
        print(f"[ERROR] Al ejecutar control de alertas: {e}")

# menu.py
def opt_11_listar_alertas_mongo():
    try:
        mon.bootstrap_indexes()
        coll = mon.db["alerts"]  # reutilizo el handle de mongo_part
        cur = coll.find().sort([("_id", -1)]).limit(50)
        print("\nÚltimas alertas (MongoDB):")
        print("-"*100)
        for a in cur:
            sid = a.get("sensor_id","-")
            typ = a.get("type","-")
            msg = a.get("description") or a.get("message","")
            ts  = a.get("timestamp") or a.get("at","")
            st  = a.get("status","")
            print(f"[{st:<7}] {ts:<25} sensor={sid:<12} tipo={typ:<10} {msg}")
    except Exception as e:
        print(f"[ERROR] Listando alertas: {e}")


def opt_10_run_report_averages():
    email = ask("Email usuario", "nahuel@ejemplo.com")
    name = ask("Nombre", "Usuario Prueba")
    uid = guard(lambda: mon.create_user(email, name, "pwd$cli", roles=["usuario"]))
    if not uid:
        return

    country = ask("País (ISO2)", "AR")
    city = ask("Ciudad", "Buenos Aires")
    year = ask("Año", now_ba().year, int)
    month = ask("Mes (1-12)", now_ba().month, int)

    out = app.run_monthly_avg_report(uid, country, city, year, month)
    if out and out.get("ok", True):
        print_ok("Reporte de promedios OK")
        print(out)
    else:
        print_err("Reporte de promedios fallido")

def opt_99_exit():
    print("\n¡Hasta la próxima!")
    sys.exit(0)

# Usuarios / Roles
def opt_u1_setup_roles():
    user_bootstrap()
    seed_basic_roles()
    print_ok("Roles base creados. Listo para crear usuarios.")

def opt_u2_crear_usuario():
    nombre = ask("Nombre completo", "Usuario Demo")
    email = ask("Email", "demo@uade.com")
    pwd = ask("Password", "pwd$demo")
    roles = [r.strip() for r in ask("Roles (coma)", "usuario").split(",") if r.strip()]
    print(create_user(nombre, email, pwd, roles))

def opt_u3_asignar_rol():
    email = ask("Email", "demo@uade.com")
    rol = ask("Rol a asignar", "tecnico")
    print(assign_role(email, rol))

# Sesiones
def opt_s1_login():
    email = ask("Email", "demo@uade.com")
    pwd = ask("Password", "pwd$demo")
    sess = login(email, pwd)
    print_ok(f"Sesión iniciada. session_id={sess['session_id']} roles={sess['roles']}")

def opt_s2_logout():
    email = ask("Email para cerrar sesiones", "demo@uade.com")
    print_ok("Logout OK" if logout(email) else "No había sesiones activas")

def opt_s3_listar_sesiones():
    email = ask("Filtrar por email (vacío=todo)", "")
    email = email if email else None
    for s in get_active_sessions(email):
        print(s)

# Mensajería
def opt_m1_crear_grupo():
    nombre = ask("Nombre del grupo", "Mantenimiento")
    emails = [m.strip() for m in ask("Miembros email (coma)", "").split(",") if m.strip()]
    miembros: list[str] = []
    for e in emails:
        try:
            uid = _user_by_email(e)["user_id"]
            miembros.append(uid)
        except Exception:
            print_err(f"⚠︎ Email no encontrado o sin user_id: {e}. Se omite.")
    if not miembros:
        raise ValueError("El grupo debe tener al menos un miembro válido.")
    doc = create_group(nombre, miembros)
    print_ok(f"Grupo creado id={doc['group_id']} con {len(miembros)} miembro(s)")


def opt_m2_enviar_privado():
    sid = ask("Session ID (del emisor)", cast=int)
    _require_perm(sid, "message:send")
    from_email = ask("Tu email (emisor)", "demo@uade.com")
    to_email = ask("Email destinatario", "usr@uade.com")
    content = ask("Contenido", "Hola!")
    from_uid = _user_by_email(from_email)["user_id"]
    to_uid = _user_by_email(to_email)["user_id"]
    m = send_private_message(from_uid, to_uid, content)
    print_ok(f"Mensaje privado id={m['message_id']} enviado")

def opt_m3_enviar_grupal():
    sid = ask("Session ID (del emisor)", cast=int)
    _require_perm(sid, "message:send")
    from_email = ask("Tu email (emisor)", "demo@uade.com")
    group_id = ask("group_id (numérico)", None, int)
    content = ask("Contenido", "Recordatorio de mantenimiento")
    from_uid = _user_by_email(from_email)["user_id"]
    m = send_group_message(from_uid, group_id, content)
    print_ok(f"Mensaje grupal id={m['message_id']} enviado al grupo {group_id}")

def opt_m4_ver_inbox():
    email = ask("Tu email", "usr@uade.com")
    uid = _user_by_email(email)["user_id"]
    msgs = list_inbox(uid)
    if not msgs:
        print("(Inbox vacío)")
    for m in msgs:
        print(f"[{m['message_id']}] {m['type']} -> {m.get('to_user_id') or 'grupo ' + str(m.get('group_id'))}: {m['content']}")

def opt_m5_marcar_leido():
    email = ask("Tu email", "usr@uade.com")
    uid = _user_by_email(email)["user_id"]
    mid = ask("message_id (numérico)", None, int)
    print_ok("OK" if mark_as_read(uid, mid) else "No cambió (ya estaba leído?)")

# Menú
OPTIONS = [
    ("Preparar entorno (Cassandra/Mongo/Neo4j + Usuarios/Roles)", opt_1_bootstrap_all),

    # Usuarios / Roles
    ("Usuarios: inicializar roles base", opt_u1_setup_roles),
    ("Usuarios: crear usuario", opt_u2_crear_usuario),
    ("Usuarios: asignar rol", opt_u3_asignar_rol),

    # Sesiones
    ("Sesión: login", opt_s1_login),
    ("Sesión: logout por email", opt_s2_logout),
    ("Sesión: listar activas", opt_s3_listar_sesiones),

    # Mensajería
    ("Mensajes: crear grupo", opt_m1_crear_grupo),
    ("Mensajes: enviar privado", opt_m2_enviar_privado),
    ("Mensajes: enviar a grupo", opt_m3_enviar_grupal),
    ("Mensajes: ver inbox", opt_m4_ver_inbox),
    ("Mensajes: marcar como leído", opt_m5_marcar_leido),

    # Data / Procesos
    ("Cassandra: demo (insertar y consultar)", opt_2_cassandra),
    ("Mongo: crear usuario + sesión (demo)", opt_3_mongo_create_user_and_session),
    ("Neo4j: bootstrap + seeds + admin", opt_4_neo_bootstrap_seed_admin),
    ("Reporte mensual (máx/mín por ciudad)", opt_5_run_report_extremes),
    ("Reporte mensual (promedios)", opt_10_run_report_averages),

    # Alertas / Estado
    ("Redis: crear y ver alerta (demo)", opt_6_redis_alert_flow),
    ("Neo4j: listar procesos por usuario", opt_7_neo_user_perms),
    ("Ver alertas (actividad + límites)", opt_9_alertas_check),
    ("Ver alertas guardadas (MongoDB)", opt_11_listar_alertas_mongo),

    ("Salir", opt_99_exit),
]

def main():
    while True:
        print("\n")
        print("  TP - Persistencia Políglota ")
        print(" --- MENÚ -- ")
        for i, (label, _) in enumerate(OPTIONS, start=1):
            print(f"{i}. {label}")
        try:
            choice = int(ask("Elegí una opción", 1, int))
        except ValueError:
            print_err("Opción inválida")
            continue

        if 1 <= choice <= len(OPTIONS):
            _, fn = OPTIONS[choice - 1]
            fn()
            pause()
        else:
            print_err("Opción fuera de rango")

if __name__ == "__main__":
    main()
