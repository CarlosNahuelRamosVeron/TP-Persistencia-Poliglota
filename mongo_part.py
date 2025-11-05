from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure
from datetime import datetime
import argparse
import os


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "tp_poliglota")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def _ensure_index_safe(coll, keys, **opts):
    try:
        coll.create_index(keys, **opts)
    except OperationFailure as e:
        if getattr(e, "code", None) == 85:  # IndexOptionsConflict
            return
        raise


def bootstrap_indexes():
    _ensure_index_safe(db.processes, [("name", ASCENDING)], name="ix_processes_name")

    _ensure_index_safe(
        db.requests,
        [("userId", ASCENDING), ("status", ASCENDING), ("requestedAt", DESCENDING)],
        name="ix_requests_user_status_time",
    )
    _ensure_index_safe(
        db.requests,
        [("processId", ASCENDING), ("requestedAt", DESCENDING)],
        name="ix_requests_process_time",
    )

    _ensure_index_safe(db.executions, [("requestId", ASCENDING)], name="ix_exec_request")
    _ensure_index_safe(db.executions, [("startedAt", DESCENDING)], name="ix_exec_started")
    _ensure_index_safe(
        db.executions, [("status", ASCENDING), ("finishedAt", DESCENDING)], name="ix_exec_status_time"
    )

    _ensure_index_safe(db.invoices, [("userId", ASCENDING), ("issuedAt", DESCENDING)], name="ix_invoice_user_time")
    _ensure_index_safe(db.invoices, [("status", ASCENDING)], name="ix_invoice_status")

    _ensure_index_safe(db.payments, [("invoiceId", ASCENDING), ("paidAt", DESCENDING)], name="ix_payment_invoice_time")

    _ensure_index_safe(db.accounts, [("userId", ASCENDING)], unique=True, name="ux_account_user")

    print("Índices creados/validados en MongoDB")

def create_user(email, fullName, passwordHash, roles=["usuario"]):
    uid    = f"usr_{abs(hash(email))}"
    acc_id = f"cta_{abs(hash(uid))}"
    now    = datetime.utcnow()

    doc = {
        "_id": uid,
        "fullName": fullName,
        "email": email,
        "passwordHash": passwordHash,
        "status": "activo",
        "registeredAt": now,
        "roles": roles,
        "accountId": acc_id
    }
    try:
        db.users.insert_one(doc)
    except Exception:
        db.users.update_one(
            {"_id": uid},
            {
                "$set": {"fullName": fullName, "roles": roles, "status": "activo"},
                "$setOnInsert": {"email": email, "passwordHash": passwordHash, "registeredAt": now},
            },
            upsert=True,
        )

    db.accounts.update_one(
        {"_id": acc_id},
        {"$setOnInsert": {"userId": uid, "balance": 0.0, "movements": []}},
        upsert=True,
    )
    db.users.update_one({"_id": uid}, {"$set": {"accountId": acc_id}})
    print(f"Usuario creado o actualizado: {email} (id={uid})")
    return uid

def open_session(userId, role):
    ses = {
        "_id": f"ses_{abs(hash(userId + role + str(datetime.utcnow())))}",
        "userId": userId,
        "role": role,
        "startedAt": datetime.utcnow(),
        "closedAt": None,
        "status": "activa",
    }
    db.sessions.insert_one(ses)
    print(f"Sesión abierta para {userId} con rol {role}")
    return ses["_id"]

def define_process(pid, name, description, baseCost: float, ptype="reporte", paramsSpec=None):
    db.processes.update_one(
        {"_id": pid},
        {
            "$set": {
                "name": name,
                "description": description,
                "baseCost": baseCost,
                "type": ptype,
                "paramsSpec": paramsSpec or {},
            }
        },
        upsert=True,
    )
    print(f"Proceso definido: {pid} ({name})")

def request_process(userId, processId, params):
    req = {
        "_id": f"req_{abs(hash((userId, processId, str(params))))}",
        "userId": userId,
        "processId": processId,
        "requestedAt": datetime.utcnow(),
        "status": "pendiente",
        "params": params,
    }
    db.requests.insert_one(req)
    print(f"Solicitud creada: {req['_id']} para proceso {processId}")
    return req["_id"]

def record_execution(requestId, ok=True, resultLocation=None, meteredUnits=0, notes=""):
    execd = {
        "_id": f"exec_{abs(hash((requestId, datetime.utcnow().isoformat())))}",
        "requestId": requestId,
        "startedAt": datetime.utcnow(),
        "finishedAt": datetime.utcnow(),
        "status": "ok" if ok else "fallido",
        "resultLocation": resultLocation,
        "meteredUnits": meteredUnits,
        "notes": notes,
    }
    db.executions.insert_one(execd)
    db.requests.update_one({"_id": requestId}, {"$set": {"status": "completado" if ok else "fallido"}})
    print(f"Ejecución registrada para request {requestId} ({'OK' if ok else 'FALLIDO'})")
    return execd["_id"]

def issue_invoice(userId, items):
    subtotal = sum(i["amount"] for i in items)
    tax = round(subtotal * 0.21, 2)
    total = round(subtotal + tax, 2)
    inv = {
        "_id": f"fac_{abs(hash((userId, datetime.utcnow().isoformat())))}",
        "userId": userId,
        "issuedAt": datetime.utcnow(),
        "items": items,
        "subtotal": subtotal,
        "tax": tax,
        "total": total,
        "status": "pendiente",
    }
    db.invoices.insert_one(inv)
    db.accounts.update_one(
        {"userId": userId},
        {
            "$inc": {"balance": -total},
            "$push": {"movements": {"ts": datetime.utcnow(), "type": "invoice", "ref": inv["_id"], "amount": -total}},
        },
    )
    print(f"Factura emitida {inv['_id']} (total: ${total})")
    return inv["_id"], total

def register_payment(invoiceId, amount, method="tarjeta"):
    pay = {
        "_id": f"pay_{abs(hash((invoiceId, amount, datetime.utcnow().isoformat())))}",
        "invoiceId": invoiceId,
        "paidAt": datetime.utcnow(),
        "amount": amount,
        "method": method,
    }
    db.payments.insert_one(pay)
    inv = db.invoices.find_one({"_id": invoiceId})
    db.invoices.update_one({"_id": invoiceId}, {"$set": {"status": "pagada"}})
    db.accounts.update_one(
        {"userId": inv["userId"]},
        {
            "$inc": {"balance": amount},
            "$push": {"movements": {"ts": datetime.utcnow(), "type": "payment", "ref": pay["_id"], "amount": amount}},
        },
    )
    print(f"Pago registrado {pay['_id']} por ${amount} usando {method}")
    return pay["_id"]

def insert_alerts(alerts):
    coll = db["alerts"]
    coll.insert_many(alerts)


def update_process_state(request_id: str, state: str, payload=None, notes: str = "") -> str:
    """
    Registra el estado final de un proceso (request) creando una 'execution' y
    actualiza el estado de la request.
      - state: "completed" | "failed"
      - payload: dict/obj con el resultado (ej. promedios), opcional
    Devuelve exec_id.
    """
    now = datetime.utcnow()
    status_exec = "ok" if state == "completed" else "fallido"
    status_req  = "completado" if state == "completed" else "fallido"

    exec_id = f"exec_{abs(hash((request_id, now.isoformat(), status_exec)))}"
    exec_doc = {
        "_id": exec_id,
        "requestId": request_id,
        "startedAt": now,        # simple: misma hora para start/finish
        "finishedAt": now,
        "status": status_exec,   # "ok" | "fallido"
        "resultLocation": None,
        "meteredUnits": 0,
        "notes": notes,
    }
    if payload is not None:
        exec_doc["payload"] = payload

    db.executions.insert_one(exec_doc)
    db.requests.update_one(
        {"_id": request_id},
        {"$set": {"status": status_req, "updatedAt": now}}
    )
    return exec_id



def build_parser():
    parser = argparse.ArgumentParser(description="MongoDB CLI - Poliglota (procesos/billing)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("bootstrap", help="Crea índices de procesos/facturación")

    u = sub.add_parser("create-user", help="Crea un usuario (legacy)")
    u.add_argument("--email", required=True)
    u.add_argument("--name", required=True)
    u.add_argument("--password", required=True)
    u.add_argument("--role", default="usuario")

    p = sub.add_parser("define-process", help="Define o actualiza un proceso")
    p.add_argument("--id", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--desc", required=True)
    p.add_argument("--cost", type=float, required=True)

    r = sub.add_parser("request", help="Crea una solicitud de proceso")
    r.add_argument("--user", required=True)
    r.add_argument("--process", required=True)
    r.add_argument("--country", required=True)
    r.add_argument("--city", required=True)
    r.add_argument("--from", dest="from_", required=True)
    r.add_argument("--to", required=True)

    i = sub.add_parser("invoice", help="Emite factura simple")
    i.add_argument("--user", required=True)
    i.add_argument("--process", required=True)
    i.add_argument("--qty", type=int, default=1)
    i.add_argument("--price", type=float, required=True)

    return parser

def main():
    args = build_parser().parse_args()

    if args.cmd == "bootstrap":
        bootstrap_indexes()
        return

    if args.cmd == "create-user":
        uid = create_user(args.email, args.name, args.password, [args.role])
        open_session(uid, args.role)
        return

    if args.cmd == "define-process":
        define_process(args.id, args.name, args.desc, args.cost)
        return

    if args.cmd == "request":
        params = {"country": args.country, "city": args.city, "from": args.from_, "to": args.to}
        rid = request_process(args.user, args.process, params)
        
        record_execution(rid, ok=True, resultLocation="S3://bucket/reportes/demo.pdf", meteredUnits=120)
        return

    if args.cmd == "invoice":
        inv_id, total = issue_invoice(
            args.user,
            [{"processId": args.process, "qty": args.qty, "unitPrice": args.price, "amount": args.qty * args.price}],
        )
        register_payment(inv_id, total, "tarjeta")
        return

if __name__ == "__main__":
    main()
