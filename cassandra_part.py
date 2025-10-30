# cassandra_part.py — CLI listo para pruebas
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List
from decimal import Decimal
import argparse
import os

# ---------------------------
# Config
# ---------------------------
CASS_HOSTS = os.getenv("CASS_HOSTS", "127.0.0.1").split(",")
CASS_USER  = os.getenv("CASS_USER", "cassandra")
CASS_PASS  = os.getenv("CASS_PASS", "cassandra")
KEYSPACE   = "iot"

DDL = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
AND durable_writes = true;

CREATE TABLE IF NOT EXISTS {KEYSPACE}.sensor (
  sensor_id   text PRIMARY KEY,
  name        text,
  type        text,
  lat         double,
  lon         double,
  city        text,
  country     text,
  status      text,
  started_at  timestamp
);

CREATE TABLE IF NOT EXISTS {KEYSPACE}.measurement_by_sensor_month (
  sensor_id   text,
  yyyymm      int,
  ts          timestamp,
  temperature decimal,
  humidity    decimal,
  PRIMARY KEY ((sensor_id, yyyymm), ts)
) WITH CLUSTERING ORDER BY (ts ASC);

CREATE TABLE IF NOT EXISTS {KEYSPACE}.measurement_by_city_day (
  country     text,
  city        text,
  yyyymmdd    int,
  ts          timestamp,
  sensor_id   text,
  temperature decimal,
  humidity    decimal,
  PRIMARY KEY ((country, city, yyyymmdd), ts, sensor_id)
) WITH CLUSTERING ORDER BY (ts ASC, sensor_id ASC);

CREATE TABLE IF NOT EXISTS {KEYSPACE}.daily_city_stats (
  country     text,
  city        text,
  yyyymmdd    int,
  temp_min    decimal,
  temp_max    decimal,
  temp_avg    decimal,
  hum_min     decimal,
  hum_max     decimal,
  hum_avg     decimal,
  samples     int,
  PRIMARY KEY ((country, city), yyyymmdd)
) WITH CLUSTERING ORDER BY (yyyymmdd DESC);

CREATE TABLE IF NOT EXISTS {KEYSPACE}.sensor_health (
  sensor_id   text,
  checked_at  timestamp,
  status      text,
  notes       text,
  PRIMARY KEY (sensor_id, checked_at)
) WITH CLUSTERING ORDER BY (checked_at DESC);
"""

# ---------------------------
# Helpers
# ---------------------------
BA_TZ = timezone(timedelta(hours=-3))  # Buenos Aires (UTC-3)

def connect():
    auth = PlainTextAuthProvider(username=CASS_USER, password=CASS_PASS)
    cluster = Cluster(CASS_HOSTS, auth_provider=auth)
    session = cluster.connect()
    return session

def bootstrap_schema():
    s = connect()
    for stmt in [p for p in DDL.split(";") if p.strip()]:
        s.execute(stmt + ";")
    s.set_keyspace(KEYSPACE)
    return s

def yyyymm(dt: datetime) -> int:
    return dt.year * 100 + dt.month

def yyyymmdd(dt: datetime) -> int:
    return dt.year * 10000 + dt.month * 100 + dt.day

def to_float(x):
    return float(x) if isinstance(x, (Decimal, float, int)) else None

def normalize_humidity(h) -> Optional[float]:
    """Acepta 0..1 o 0..100 y devuelve 0..1; None si no válido."""
    if h is None:
        return None
    try:
        h = float(h)
    except Exception:
        return None
    if 0.0 <= h <= 1.0:
        return h
    if 1.0 < h <= 100.0:
        return h / 100.0
    return None

# ---------------------------
# Core ops
# ---------------------------
def upsert_sensor(s, sensor: Dict):
    s.execute("""
    INSERT INTO sensor (sensor_id,name,type,lat,lon,city,country,status,started_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (sensor["sensor_id"], sensor["name"], sensor["type"], sensor["lat"], sensor["lon"],
          sensor["city"], sensor["country"], sensor["status"], sensor["started_at"]))

def insert_measurement(s, sensor_id: str, when: datetime, temperature: Optional[float], humidity: Optional[float],
                       city: str, country: str):
    m_yyyymm   = yyyymm(when)
    m_yyyymmdd = yyyymmdd(when)
    s.execute("""
      INSERT INTO measurement_by_sensor_month (sensor_id, yyyymm, ts, temperature, humidity)
      VALUES (%s,%s,%s,%s,%s)
    """, (sensor_id, m_yyyymm, when, temperature, humidity))
    s.execute("""
      INSERT INTO measurement_by_city_day (country, city, yyyymmdd, ts, sensor_id, temperature, humidity)
      VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (country, city, m_yyyymmdd, when, sensor_id, temperature, humidity))

def query_sensor_range(s, sensor_id: str, start: datetime, end: datetime):
    # cubrir salto de mes
    months = set([yyyymm(start)])
    cursor = datetime(start.year, start.month, 1)
    while cursor <= end:
        months.add(yyyymm(cursor))
        cursor = cursor.replace(day=1)
        m = cursor.month + 1
        y = cursor.year + (1 if m == 13 else 0)
        m = 1 if m == 13 else m
        cursor = datetime(y, m, 1)
    rows: List = []
    for m in sorted(months):
        rs = s.execute("""
           SELECT ts, temperature, humidity
           FROM measurement_by_sensor_month
           WHERE sensor_id=%s AND yyyymm=%s AND ts >= %s AND ts <= %s
        """, (sensor_id, m, start, end))
        rows.extend(rs)
    return rows

def rollup_daily_for_city(s, country: str, city: str, day_yyyymmdd: int):
    rs = s.execute("""
       SELECT temperature, humidity FROM measurement_by_city_day
       WHERE country=%s AND city=%s AND yyyymmdd=%s
    """, (country, city, day_yyyymmdd))
    temps, hums = [], []
    for r in rs:
        if r.temperature is not None: temps.append(float(r.temperature))
        if r.humidity   is not None: hums.append(float(r.humidity))
    if not temps and not hums:
        return None
    temp_min = min(temps) if temps else None
    temp_max = max(temps) if temps else None
    temp_avg = sum(temps)/len(temps) if temps else None
    hum_min  = min(hums)  if hums  else None
    hum_max  = max(hums)  if hums  else None
    hum_avg  = sum(hums)/len(hums) if hums else None
    samples  = len(temps) + len(hums)
    s.execute("""
      INSERT INTO daily_city_stats (country,city,yyyymmdd,temp_min,temp_max,temp_avg,hum_min,hum_max,hum_avg,samples)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (country, city, day_yyyymmdd, temp_min, temp_max, temp_avg, hum_min, hum_max, hum_avg, samples))
    return True

# ---------------------------
# Pretty print (hora local BA)
# ---------------------------
def print_measurements(rows, use_ba_time: bool = True):
    if not rows:
        print("Mediciones recientes: (sin resultados)")
        return
    label = "Hora (BA)" if use_ba_time else "Hora (UTC)"
    ancho = 54
    print("\nMediciones recientes")
    print("-" * ancho)
    print(f"{label:<23} {'Temp (°C)':>12} {'Hum (%)':>10}")
    print("-" * ancho)
    temps, hums = [], []
    for r in rows:
        t = to_float(r.temperature)
        h = to_float(r.humidity)
        if t is not None: temps.append(t)
        if h is not None: hums.append(h)
        ts = r.ts
        if use_ba_time:
            ts = ts.replace(tzinfo=timezone.utc).astimezone(BA_TZ)
        stamp = ts.strftime('%Y-%m-%d %H:%M:%S')
        hum_pct = "" if h is None else f"{h*100:>9.1f}"
        tmp_txt = "" if t is None else f"{t:>11.2f}"
        print(f"{stamp:<23} {tmp_txt} {hum_pct:>10}")
    print("-" * ancho)
    if temps:
        print(f"Temp  -> min: {min(temps):.2f}  max: {max(temps):.2f}  avg: {sum(temps)/len(temps):.2f}")
    if hums:
        print(f"Humed -> min: {min(hums)*100:.1f}%  max: {max(hums)*100:.1f}%  avg: {sum(hums)/len(hums)*100:.1f}%")
    print()

# ---------------------------
# CLI
# ---------------------------
def build_parser():
    p = argparse.ArgumentParser(description="Cassandra IoT CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    s_boot = sub.add_parser("bootstrap", help="Crear keyspace y tablas")
    # sin args

    s_up = sub.add_parser("upsert-sensor", help="Crear/actualizar un sensor")
    s_up.add_argument("--sensor-id", required=True)
    s_up.add_argument("--name", required=True)
    s_up.add_argument("--type", default="mixto")
    s_up.add_argument("--lat", type=float, required=True)
    s_up.add_argument("--lon", type=float, required=True)
    s_up.add_argument("--city", required=True)
    s_up.add_argument("--country", required=True)
    s_up.add_argument("--status", default="activo")
    s_up.add_argument("--started-at", help="ISO datetime local BA (opcional)")

    s_im = sub.add_parser("insert-measure", help="Insertar medición")
    s_im.add_argument("--sensor-id", required=True)
    s_im.add_argument("--city", required=True)
    s_im.add_argument("--country", required=True)
    s_im.add_argument("--temp", type=float, required=False)
    s_im.add_argument("--hum", type=float, required=False, help="0..1 o 0..100")
    s_im.add_argument("--when", help="ISO datetime local BA, ej: 2025-10-29 19:52:00")

    s_q = sub.add_parser("query", help="Consultar mediciones por rango")
    s_q.add_argument("--sensor-id", required=True)
    s_q.add_argument("--from", dest="date_from", required=False, help="YYYY-MM-DD (local BA)")
    s_q.add_argument("--to",   dest="date_to",   required=False, help="YYYY-MM-DD (local BA)")
    s_q.add_argument("--utc", action="store_true", help="Mostrar en UTC (default: BA local)")

    s_ru = sub.add_parser("rollup", help="Generar rollup diario para una ciudad")
    s_ru.add_argument("--country", required=True)
    s_ru.add_argument("--city", required=True)
    s_ru.add_argument("--yyyymmdd", type=int, required=True)

    s_demo = sub.add_parser("demo", help="Demo rápida: crea sensor, inserta una medición y consulta")
    # sin args

    return p

def parse_local_ba(ts_str: Optional[str]) -> datetime:
    """Recibe 'YYYY-MM-DD HH:MM:SS' o ISO; asume hora local BA y devuelve UTC naive."""
    if not ts_str:
        return datetime.utcnow()
    # Permitir 'YYYY-MM-DD' solo fecha -> 12:00 local para evitar borde
    try:
        if len(ts_str.strip()) == 10:
            dt_local = datetime.strptime(ts_str.strip(), "%Y-%m-%d").replace(hour=12, minute=0, second=0)
        else:
            # intentar con segundos o sin segundos
            try:
                dt_local = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                dt_local = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
        # marcar como BA local y convertir a UTC
        dt_local = dt_local.replace(tzinfo=BA_TZ)
        dt_utc = dt_local.astimezone(timezone.utc).replace(tzinfo=None)
        return dt_utc
    except Exception:
        # fallback: ahora UTC
        return datetime.utcnow()

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "bootstrap":
        s = bootstrap_schema()
        print(f"OK: keyspace/tablas creadas en '{KEYSPACE}' (hosts={CASS_HOSTS})")
        return

    s = bootstrap_schema()

    if args.cmd == "upsert-sensor":
        started_at_utc = parse_local_ba(args.started_at) if args.started_at else datetime.utcnow()
        upsert_sensor(s, {
            "sensor_id": args.sensor_id, "name": args.name, "type": args.type,
            "lat": args.lat, "lon": args.lon, "city": args.city, "country": args.country,
            "status": args.status, "started_at": started_at_utc
        })
        print(f"OK: sensor {args.sensor_id} upserted.")
        return

    if args.cmd == "insert-measure":
        hum_norm = normalize_humidity(args.hum)
        when_utc = parse_local_ba(args.when)
        insert_measurement(s, args.sensor_id, when_utc, args.temp, hum_norm, args.city, args.country)
        print(f"OK: medición insertada para {args.sensor_id} @ {when_utc.isoformat()}Z (UTC)")
        return

    if args.cmd == "query":
        # rango por defecto: hoy 00:00 BA -> ahora
        if args.date_from:
            start_utc = parse_local_ba(args.date_from + " 00:00:00")
        else:
            now_ba = datetime.now(BA_TZ)
            start_utc = datetime(now_ba.year, now_ba.month, now_ba.day, 0, 0, 0, tzinfo=BA_TZ).astimezone(timezone.utc).replace(tzinfo=None)
        if args.date_to:
            end_utc = parse_local_ba(args.date_to + " 23:59:59")
        else:
            end_utc = datetime.utcnow()
        rows = list(query_sensor_range(s, args.sensor_id, start_utc, end_utc))
        print_measurements(rows, use_ba_time=not args.utc)
        return

    if args.cmd == "rollup":
        ok = rollup_daily_for_city(s, args.country, args.city, args.yyyymmdd)
        if ok:
            print(f"OK: rollup generado para {args.city} ({args.country}) día {args.yyyymmdd}")
        else:
            print(f"Sin datos para {args.city} ({args.country}) día {args.yyyymmdd}")
        return

    if args.cmd == "demo":
        # Crea un sensor en el Obelisco, inserta medición ahora y consulta el día
        sensor_id = "S-AR-0001"
        upsert_sensor(s, {
            "sensor_id": sensor_id, "name": "Obelisco", "type": "mixto",
            "lat": -34.6037, "lon": -58.3816, "city": "Buenos Aires", "country": "AR",
            "status": "activo", "started_at": datetime.utcnow()
        })
        now_ba = datetime.now(BA_TZ).replace(microsecond=0)
        when_utc = now_ba.astimezone(timezone.utc).replace(tzinfo=None)
        insert_measurement(s, sensor_id, when_utc, 22.8, 0.55, "Buenos Aires", "AR")
        start_ba = now_ba.replace(hour=0, minute=0, second=0)
        start_utc = start_ba.astimezone(timezone.utc).replace(tzinfo=None)
        rows = list(query_sensor_range(s, sensor_id, start_utc, when_utc))
        print_measurements(rows, use_ba_time=True)
        rollup_daily_for_city(s, "AR", "Buenos Aires", yyyymmdd(when_utc))
        print("Demo OK.")
        return

if __name__ == "__main__":
    main()
