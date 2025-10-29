
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
import os
from pymongo.errors import DuplicateKeyError


MONGO_URI = os.getenv("MONGO_URI","mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB","poliglot")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def bootstrap_indexes():
    db.users.create_index([("email", ASCENDING)], unique=True)
    db.users.create_index([("status", ASCENDING), ("registeredAt", DESCENDING)])

    db.sessions.create_index([("userId", ASCENDING), ("status", ASCENDING)])
    # TTL opcional: expirar sesiones 30 días después de startedAt
    db.sessions.create_index([("startedAt", ASCENDING)], expireAfterSeconds=30*24*3600)

    db.messages.create_index([("to.type", ASCENDING), ("to.id", ASCENDING), ("sentAt", DESCENDING)])
    db.messages.create_index([("fromUser", ASCENDING), ("sentAt", DESCENDING)])

    db.requests.create_index([("userId", ASCENDING), ("status", ASCENDING), ("requestedAt", DESCENDING)])
    db.executions.create_index([("requestId", ASCENDING)])
    db.processes.create_index([("type", ASCENDING)])

    db.invoices.create_index([("userId", ASCENDING), ("issuedAt", DESCENDING)])
    db.invoices.create_index([("status", ASCENDING)])
    db.payments.create_index([("invoiceId", ASCENDING), ("paidAt", DESCENDING)])
    db.accounts.create_index([("userId", ASCENDING)], unique=True)

def create_user(email, fullName, passwordHash, roles=["usuario"]):
    uid    = f"usr_{abs(hash(email))}"
    acc_id = f"cta_{abs(hash(uid))}"
    now    = datetime.utcnow()

    # 1) Intentar crear el usuario (si ya existe por email/uid, lo reutilizamos)
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
    except DuplicateKeyError:
        # Ya existe: solo aseguramos datos mínimos y NO fallamos
        db.users.update_one(
            {"_id": uid},
            {"$set": {
                "fullName": fullName,
                "roles": roles,
                "status": "activo"
            },
             "$setOnInsert": {
                "email": email,
                "passwordHash": passwordHash,
                "registeredAt": now
             }}
        )
    # 2) Asegurar cuenta corriente (upsert, no falla si ya existe)
    db.accounts.update_one(
        {"_id": acc_id},
        {"$setOnInsert": {
            "userId": uid,
            "balance": 0.0,
            "movements": []
        }},
        upsert=True
    )
    # 3) Asegurar que el user apunte a su cuenta
    db.users.update_one({"_id": uid}, {"$set": {"accountId": acc_id}})

    return uid


def open_session(userId, role):
    ses = {"_id": f"ses_{abs(hash(userId+role+str(datetime.utcnow()))) }",
           "userId": userId, "role": role, "startedAt": datetime.utcnow(),
           "closedAt": None, "status": "activa"}
    db.sessions.insert_one(ses)
    return ses["_id"]

def define_process(pid, name, description, baseCost: float, ptype="reporte", paramsSpec=None):
    db.processes.update_one({"_id": pid}, {"$set":{
        "name": name, "description": description, "baseCost": baseCost,
        "type": ptype, "paramsSpec": paramsSpec or {}
    }}, upsert=True)

def request_process(userId, processId, params):
    req = {"_id": f"req_{abs(hash((userId, processId, str(params))))}",
           "userId": userId, "processId": processId, "requestedAt": datetime.utcnow(),
           "status": "pendiente", "params": params}
    db.requests.insert_one(req)
    return req["_id"]

def record_execution(requestId, ok=True, resultLocation=None, meteredUnits=0, notes=""):
    execd = {"_id": f"exec_{abs(hash((requestId, datetime.utcnow().isoformat()))) }",
             "requestId": requestId, "startedAt": datetime.utcnow(),
             "finishedAt": datetime.utcnow(), "status": "ok" if ok else "fallido",
             "resultLocation": resultLocation, "meteredUnits": meteredUnits, "notes": notes}
    db.executions.insert_one(execd)
    db.requests.update_one({"_id": requestId}, {"$set":{"status": "completado" if ok else "fallido"}})
    return execd["_id"]

def issue_invoice(userId, items):
    subtotal = sum(i["amount"] for i in items)
    tax = round(subtotal*0.21, 2)
    total = round(subtotal+tax, 2)
    inv = {"_id": f"fac_{abs(hash((userId, datetime.utcnow().isoformat()))) }",
           "userId": userId, "issuedAt": datetime.utcnow(),
           "items": items, "subtotal": subtotal, "tax": tax, "total": total, "status":"pendiente"}
    db.invoices.insert_one(inv)
    # asiento en cuenta corriente
    db.accounts.update_one({"userId": userId}, {
        "$inc": {"balance": -total},
        "$push": {"movements": {"ts": datetime.utcnow(), "type":"invoice", "ref": inv["_id"], "amount": -total}}
    })
    return inv["_id"], total

def register_payment(invoiceId, amount, method="tarjeta"):
    pay = {"_id": f"pay_{abs(hash((invoiceId, amount, datetime.utcnow().isoformat()))) }",
           "invoiceId": invoiceId, "paidAt": datetime.utcnow(), "amount": amount, "method": method}
    db.payments.insert_one(pay)
    inv = db.invoices.find_one({"_id": invoiceId})
    db.invoices.update_one({"_id": invoiceId}, {"$set":{"status":"pagada"}})
    # reflejar en cuenta corriente
    db.accounts.update_one({"userId": inv["userId"]}, {
        "$inc": {"balance": amount},
        "$push": {"movements": {"ts": datetime.utcnow(), "type":"payment", "ref": pay["_id"], "amount": amount}}
    })
    return pay["_id"]

if __name__ == "__main__":
    bootstrap_indexes()
    uid = create_user("nahuel@example.com", "Nahuel Veron", "cnrv$demo", roles=["usuario"])
    sid = open_session(uid, "usuario")
    define_process("proc_temp_max_min", "Max/Min por ciudad", "Informe de extremos", 10.0,
                   paramsSpec={"country":"string","city":"string","from":"date","to":"date","granularity":["daily","monthly"]})
    rid = request_process(uid, "proc_temp_max_min", {"country":"AR","city":"Buenos Aires","from":"2025-01-01","to":"2025-01-31","granularity":"monthly"})
    eid = record_execution(rid, ok=True, resultLocation="s3://bucket/reportes/rid.pdf", meteredUnits=1200)
    inv_id, total = issue_invoice(uid, [{"processId":"proc_temp_max_min","qty":1,"unitPrice":10.0,"amount":10.0}])
    pay_id = register_payment(inv_id, total, "tarjeta")
    print("OK Mongo:", uid, sid, rid, eid, inv_id, pay_id)
