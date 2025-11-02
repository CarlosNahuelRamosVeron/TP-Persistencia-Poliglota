
import os, json, time, argparse
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from redis import Redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = Redis.from_url(REDIS_URL, decode_responses=True)


BA_TZ = timezone(timedelta(hours=-3))

def parse_when_to_epoch(when: Optional[str]) -> int:
    if not when:
        return int(time.time())
    when = str(when).strip()
    if when.isdigit():
        return int(when)
    try:
        if len(when) == 10:
            dt_local = datetime.strptime(when, "%Y-%m-%d").replace(hour=12, minute=0, second=0)
        else:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                try:
                    dt_local = datetime.strptime(when, fmt)
                    break
                except ValueError:
                    dt_local = None
            if dt_local is None:
                raise ValueError("Formato de fecha inválido")
        dt_local = dt_local.replace(tzinfo=BA_TZ)
        dt_utc = dt_local.astimezone(timezone.utc)
        return int(dt_utc.timestamp())
    except Exception:
        return int(time.time())

def fmt_epoch_local(epoch: int) -> str:
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(BA_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def push_alert(alert_id: str, at_epoch: int, payload: Dict[str, Any]):
    r.hset(f"alert:{alert_id}", mapping={
        "type": payload.get("type", "sensor"),
        "sensor_id": payload.get("sensor_id", ""),
        "ts": at_epoch,
        "desc": payload.get("desc", ""),
        "state": "activa"
    })
    r.zadd("alerts:active", {f"alert:{alert_id}": at_epoch})
    r.xadd("alerts:queue", {"id": alert_id, "json": json.dumps(payload)})

def resolve_alert(alert_id: str):
    r.hset(f"alert:{alert_id}", mapping={"state": "resuelta"})
    r.zrem("alerts:active", f"alert:{alert_id}")

def cache_session(token: str, userId: str, ttl_secs: int = 3600):
    r.hset(f"sessions:{token}", mapping={"userId": userId, "token": token})
    r.expire(f"sessions:{token}", ttl_secs)

def get_session(token: str) -> Optional[Dict[str, Any]]:
    d = r.hgetall(f"sessions:{token}")
    return d or None

def del_session(token: str) -> int:
    return r.delete(f"sessions:{token}")

def incr_usage(userId: str, yyyymm: str, units: int):
    r.incrby(f"usage:{userId}:{yyyymm}", units)

def get_usage(userId: str, yyyymm: str) -> int:
    v = r.get(f"usage:{userId}:{yyyymm}")
    return int(v) if v else 0


def build_parser():
    p = argparse.ArgumentParser(description="Redis CLI - Alerts/Sessions/Usage")
    sub = p.add_subparsers(dest="cmd", required=True)

    a1 = sub.add_parser("push-alert", help="Crear alerta y encolarla")
    a1.add_argument("--id", required=True, help="alert_id")
    a1.add_argument("--type", default="sensor")
    a1.add_argument("--sensor-id", default="")
    a1.add_argument("--desc", default="")
    a1.add_argument("--when", help="epoch o 'YYYY-MM-DD [HH:MM[:SS]]' (local BA)")

    a2 = sub.add_parser("resolve-alert", help="Resolver alerta")
    a2.add_argument("--id", required=True)

    a3 = sub.add_parser("list-alerts", help="Listar alertas activas")
    a3.add_argument("--limit", type=int, default=50)

    a4 = sub.add_parser("tail-queue", help="Leer del stream alerts:queue")
    a4.add_argument("--count", type=int, default=10)
    a4.add_argument("--block-ms", type=int, default=2000)
    a4.add_argument("--from-id", default="$", help="Use '0-0' para leer histórico, '$' para nuevos")

    s1 = sub.add_parser("cache-session", help="Cachear sesión con TTL")
    s1.add_argument("--token", required=True)
    s1.add_argument("--user", required=True)
    s1.add_argument("--ttl", type=int, default=3600)

    s2 = sub.add_parser("get-session", help="Obtener sesión")
    s2.add_argument("--token", required=True)

    s3 = sub.add_parser("del-session", help="Eliminar sesión")
    s3.add_argument("--token", required=True)

    u1 = sub.add_parser("incr-usage", help="Incrementar uso")
    u1.add_argument("--user", required=True)
    u1.add_argument("--yyyymm", required=True)
    u1.add_argument("--units", type=int, required=True)

    u2 = sub.add_parser("get-usage", help="Consultar uso")
    u2.add_argument("--user", required=True)
    u2.add_argument("--yyyymm", required=True)

    st = sub.add_parser("stats", help="Resumen de estado")
    fl = sub.add_parser("flush", help="Borra claves del namespace (alerts/sessions/usage)")

    return p

def main():
    args = build_parser().parse_args()

    if args.cmd == "push-alert":
        ts = parse_when_to_epoch(args.when)
        payload = {"type": args.type, "sensor_id": args.sensor_id, "desc": args.desc, "ts": ts}
        push_alert(args.id, ts, payload)
        print(f"OK alerta {args.id} @ {fmt_epoch_local(ts)} (BA) / {ts} (epoch)")
        return

    if args.cmd == "resolve-alert":
        resolve_alert(args.id)
        print(f"OK alerta {args.id} marcada como resuelta")
        return

    if args.cmd == "list-alerts":
        items = r.zrevrange("alerts:active", 0, args.limit - 1, withscores=True)
        if not items:
            print("No hay alertas activas")
            return
        ancho = 90
        print("\nAlertas activas")
        print("=" * ancho)
        print(f"{'ID':<20} {'Tipo':<10} {'Sensor':<14} {'Estado':<10} {'Fecha (BA)':<20} {'Epoch':>10}")
        print("-" * ancho)
        for key, score in items:
            alert_key = key
            aid = alert_key.split("alert:", 1)[-1]
            h = r.hgetall(alert_key)
            tipo = h.get("type", "")
            sensor = h.get("sensor_id", "")
            estado = h.get("state", "")
            ts = int(float(h.get("ts", score)))
            print(f"{aid:<20} {tipo:<10} {sensor:<14} {estado:<10} {fmt_epoch_local(ts):<20} {ts:>10}")
        print("=" * ancho)
        return

    if args.cmd == "tail-queue":
        
        items = r.xread({"alerts:queue": args.from_id}, count=args.count, block=args.block_ms)
        if not items:
            print("(sin eventos nuevos)")
            return
        for stream, entries in items:
            for eid, fields in entries:
                try:
                    payload = json.loads(fields.get("json", "{}"))
                except Exception:
                    payload = {"raw": fields.get("json")}
                aid = fields.get("id")
                ts = int(payload.get("ts", time.time()))
                print(f"[{eid}] alert_id={aid} type={payload.get('type')} sensor={payload.get('sensor_id')} ts={fmt_epoch_local(ts)} BA")
        return

    if args.cmd == "cache-session":
        cache_session(args.token, args.user, args.ttl)
        ttl = r.ttl(f"sessions:{args.token}")
        print(f"OK sesión cacheada token={args.token} user={args.user} ttl={ttl}s")
        return

    if args.cmd == "get-session":
        d = get_session(args.token)
        if not d:
            print("(no encontrada)")
        else:
            ttl = r.ttl(f"sessions:{args.token}")
            print(f"token={args.token} userId={d.get('userId')} ttl={ttl}s")
        return

    if args.cmd == "del-session":
        n = del_session(args.token)
        print(f"Eliminadas: {n}")
        return

    if args.cmd == "incr-usage":
        incr_usage(args.user, args.yyyymm, args.units)
        total = get_usage(args.user, args.yyyymm)
        print(f"OK usage:{args.user}:{args.yyyymm} += {args.units} (total={total})")
        return

    if args.cmd == "get-usage":
        print(get_usage(args.user, args.yyyymm))
        return

    if args.cmd == "stats":
        n_alerts = r.zcard("alerts:active")
        n_sessions = sum(1 for _ in r.scan_iter("sessions:*"))
        n_usage = sum(1 for _ in r.scan_iter("usage:*"))
        print(f"Redis @ {REDIS_URL}")
        print(f"- alerts:active = {n_alerts}")
        print(f"- sessions:*    = {n_sessions}")
        print(f"- usage:*       = {n_usage}")
        return

    if args.cmd == "flush":
        
        deleted = 0
        for pattern in ("alert:*", "alerts:active", "alerts:queue", "sessions:*", "usage:*"):
            for k in r.scan_iter(pattern):
                deleted += r.delete(k)
        print(f"Claves borradas: {deleted}")
        return

if __name__ == "__main__":
    main()
