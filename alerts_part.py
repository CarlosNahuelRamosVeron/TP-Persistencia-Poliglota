from datetime import datetime, timedelta, timezone
import mongo_part as mon
import cassandra_part as cas
from cassandra_part import to_float


BA_TZ = timezone(timedelta(hours=-3))
TEMP_MAX = 45.0
TEMP_MIN = -10.0
HUM_MAX = 85.0
HUM_MIN = 10.0

INACTIVITY_LIMIT_HOURS = 6


def check_sensor_activity(session):
    sensors = cas.get_all_sensors(session)
    now = datetime.now(timezone.utc)
    alerts = []

    for s in sensors:
        sid = getattr(s, "sensor_id", getattr(s, "id", None))
        name = getattr(s, "name", sid)
        if not sid:
            continue

        last = cas.get_last_measurement(session, sid)

        last_dt = None
        if last and getattr(last, "when_utc", None):
            last_dt = last.when_utc
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)

        inactive = (last_dt is None) or ((now - last_dt).total_seconds() > INACTIVITY_LIMIT_HOURS * 3600)

        if inactive:
            alerts.append({
                "type": "sensor",
                "sensor_id": sid,
                "timestamp": now.isoformat(),
                "description": f"Sensor {name} inactivo desde {last_dt.isoformat() if last_dt else 'desconocido'}",
                "status": "activa"
            })

    if alerts:
        mon.insert_alerts(alerts)
        print(f"{len(alerts)} alertas de inactividad registradas.")
    else:
        print("Todos los sensores activos.")
    return len(alerts)



def check_temperature_limits(session):
    readings = cas.get_recent_measurements(session, hours=1)
    now = datetime.now(timezone.utc)
    alerts = []

    for r in readings:
        # soportar Row (atributos) o dict
        t = to_float(getattr(r, "temperature", getattr(r, "temp_c", None)) if hasattr(r, "temperature") else r.get("temperature"))
        if t is None:
            continue

        if t > TEMP_MAX:
            desc = f"Temperatura extrema alta ({t:.1f}°C)"
        elif t < TEMP_MIN:
            desc = f"Temperatura extrema baja ({t:.1f}°C)"
        else:
            continue

        alerts.append({
            "type": "climatica",
            "sensor_id": getattr(r, "sensor_id", r.get("sensor_id")),
            "timestamp": now.isoformat(),
            "description": desc,
            "status": "activa",
        })

    if alerts:
        mon.insert_alerts(alerts)
        print(f"{len(alerts)} alertas climáticas registradas.")
    else:
        print("Sin alertas climáticas.")
    return len(alerts)



def safe_get(obj, key, default=None):
    """
    Permite acceder a claves de forma segura tanto si el objeto es un Row (Cassandra)
    como si es un dict u otro tipo.
    """
    if isinstance(obj, dict):
        return obj.get(key, default)
    try:
        # Row y NamedTuple soportan acceso por atributo
        return getattr(obj, key)
    except AttributeError:
        return default

def check_humidity_limits(session, hum_min=HUM_MIN, hum_max=HUM_MAX):
    """
    Genera alertas si la humedad actual del sensor (última medición)
    está fuera de [hum_min, hum_max].
    Retorna la cantidad de alertas insertadas.
    """
    alerts = []
    now = datetime.now(BA_TZ)

    sensors = cas.get_all_sensors(session)
    for s in sensors:
        sensor_id = safe_get(s, "sensorId") or safe_get(s, "id")
        if not sensor_id:
            continue

        try:
            recents = cas.get_recent_measurements(session, sensor_id, limit=1)
        except TypeError:
            recents = cas.get_recent_measurements(session, sensor_id)
        if not recents:
            continue

        last = recents[0]
        hum = safe_get(last, "humidity") or safe_get(last, "hum")
        if hum is None:
            continue

        if hum < hum_min or hum > hum_max:
            alerts.append({
                "type": "climate",
                "subtype": "humidity_out_of_range",
                "sensorId": sensor_id,
                "country": safe_get(s, "country"),
                "city": safe_get(s, "city"),
                "value": float(hum),
                "thresholdMin": float(hum_min),
                "thresholdMax": float(hum_max),
                "at": now.isoformat(),
                "message": (
                    f"Humedad fuera de rango: {hum:.1f}% "
                    f"(umbral {hum_min:.1f}%–{hum_max:.1f}%)."
                ),
                "status": "new",
            })

    if alerts:
        mon.insert_alerts(alerts)
        print(f"{len(alerts)} alertas de humedad registradas.")
    else:
        print("Sin alertas de humedad.")
    return len(alerts)
