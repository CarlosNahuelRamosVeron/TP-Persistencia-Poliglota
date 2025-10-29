
import os, json, time
from redis import Redis

REDIS_URL = os.getenv("REDIS_URL","redis://localhost:6379/0")
r = Redis.from_url(REDIS_URL)

# Claves
# alerts:queue (Stream), alerts:active (ZSET), alert:{id} (HASH)
# sessions:{token} (STRING/HASH with TTL), usage:{userId}:{yyyyMM} (COUNTER)

def push_alert(alert_id: str, at_epoch: int, payload: dict):
    # Guardar detalle
    r.hset(f"alert:{alert_id}", mapping={
        "type": payload.get("type","sensor"),
        "sensor_id": payload.get("sensor_id",""),
        "ts": at_epoch,
        "desc": payload.get("desc",""),
        "state": "activa"
    })
    # Rank temporal
    r.zadd("alerts:active", {f"alert:{alert_id}": at_epoch})
    # Encolar evento (stream)
    r.xadd("alerts:queue", {"id": alert_id, "json": json.dumps(payload)})

def resolve_alert(alert_id: str):
    r.hset(f"alert:{alert_id}", "state", "resuelta")
    # opcional: quitar del ranking
    r.zrem("alerts:active", f"alert:{alert_id}")

def cache_session(token: str, userId: str, ttl_secs: int=3600):
    r.hset(f"sessions:{token}", mapping={"userId": userId, "token": token})
    r.expire(f"sessions:{token}", ttl_secs)

def incr_usage(userId: str, yyyymm: str, units: int):
    r.incrby(f"usage:{userId}:{yyyymm}", units)

if __name__ == "__main__":
    push_alert("alr_0001", int(time.time()), {"type":"sensor","sensor_id":"S-AR-0001","desc":"Temperatura fuera de rango"})
    cache_session("tok_123", "usr_0001", 7200)
    incr_usage("usr_0001", "202510", 5)
    print("Redis OK: active alerts", r.zcard("alerts:active"))
