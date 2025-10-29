
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
from typing import Optional, List, Dict
import math
import os
from decimal import Decimal

CASS_HOSTS = os.getenv("CASS_HOSTS","127.0.0.1").split(",")
CASS_USER  = os.getenv("CASS_USER","cassandra")
CASS_PASS  = os.getenv("CASS_PASS","cassandra")
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
    return dt.year*100 + dt.month

def yyyymmdd(dt: datetime) -> int:
    return dt.year*10000 + dt.month*100 + dt.day

def upsert_sensor(s, sensor: Dict):
    s.execute("""
    INSERT INTO sensor (sensor_id,name,type,lat,lon,city,country,status,started_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (sensor["sensor_id"], sensor["name"], sensor["type"], sensor["lat"], sensor["lon"],
          sensor["city"], sensor["country"], sensor["status"], sensor["started_at"]))

def insert_measurement(s, sensor_id: str, when: datetime, temperature: Optional[float], humidity: Optional[float],
                       city: str, country: str):
    m_yyyymm  = yyyymm(when)
    m_yyyymmdd= yyyymmdd(when)
    s.execute("""
      INSERT INTO measurement_by_sensor_month (sensor_id, yyyymm, ts, temperature, humidity)
      VALUES (%s,%s,%s,%s,%s)
    """, (sensor_id, m_yyyymm, when, temperature, humidity))
    s.execute("""
      INSERT INTO measurement_by_city_day (country, city, yyyymmdd, ts, sensor_id, temperature, humidity)
      VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (country, city, m_yyyymmdd, when, sensor_id, temperature, humidity))

def query_sensor_range(s, sensor_id: str, start: datetime, end: datetime):
    months = set([yyyymm(start)])
    # cubrir salto de mes
    cursor = datetime(start.year, start.month, 1)
    while cursor <= end:
        months.add(yyyymm(cursor))
        # avanzar 32 días para asegurar cambio
        cursor = cursor.replace(day=1)
        m = cursor.month + 1
        y = cursor.year + (1 if m==13 else 0)
        m = 1 if m==13 else m
        cursor = datetime(y, m, 1)
    rows = []
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



def to_float(x):
    return float(x) if isinstance(x, (Decimal, float, int)) else None

def print_measurements(rows):
    if not rows:
        print("Mediciones recientes: (sin resultados)")
        return
    # encabezado
    print("\nMediciones recientes")
    print("-" * 44)
    print(f"{'Hora (UTC)':<23} {'Temp (°C)':>10} {'Hum (%)':>9}")
    print("-" * 44)
    temps, hums = [], []
    for r in rows:
        t = to_float(r.temperature)
        h = to_float(r.humidity)
        if t is not None: temps.append(t)
        if h is not None: hums.append(h)
        print(f"{r.ts.strftime('%Y-%m-%d %H:%M:%S'):<23} "
              f"{'' if t is None else f'{t:>10.2f}':>10} "
              f"{'' if h is None else f'{h*100:>9.1f}':>9}")
    print("-" * 44)
    if temps:
        print(f"Temp  -> min: {min(temps):.2f}  max: {max(temps):.2f}  avg: {sum(temps)/len(temps):.2f}")
    if hums:
        print(f"Humed -> min: {min(hums)*100:.1f}%  max: {max(hums)*100:.1f}%  avg: {sum(hums)/len(hums)*100:.1f}%")
    print()


if __name__ == "__main__":
    session = bootstrap_schema()
    upsert_sensor(session, {
        "sensor_id":"S-AR-0001","name":"Obelisco","type":"mixto",
        "lat":-34.6037,"lon":-58.3816,"city":"Buenos Aires","country":"AR",
        "status":"activo","started_at":datetime.utcnow()
    })
    now = datetime.utcnow()
    insert_measurement(session, "S-AR-0001", now, 22.8, 0.55, "Buenos Aires","AR")
    rows = list(query_sensor_range(session, "S-AR-0001",
                               now.replace(hour=0, minute=0, second=0, microsecond=0),
                               now))
    print_measurements(rows)
    rollup_daily_for_city(session, "AR", "Buenos Aires", yyyymmdd(now))
