from __future__ import annotations
import os
import uuid
import json
import datetime as dt
from typing import Dict, Any, Optional, List

from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure
import redis


from users_part import _hash_password, verify_password, bootstrap_indexes as users_bootstrap


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "tp_poliglota")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SESSION_TTL_HOURS = int(os.getenv("SESSION_TTL_HOURS", "8"))

def _db():
    return MongoClient(MONGO_URI)[MONGO_DB]

def _r() -> Optional[redis.Redis]:
    try:
        return redis.from_url(REDIS_URL)
    except Exception:
        return None

def _now() -> dt.datetime:
    return dt.datetime.utcnow()

# Índices
def bootstrap_indexes():
    s = _db().sessions
    
    s.delete_many({"$or": [{"session_id": {"$exists": False}}, {"session_id": None}]})
    
    try:
        s.create_index([("session_id", ASCENDING)], unique=True, name="ux_sessions_id",
                       partialFilterExpression={"session_id": {"$type": "string"}})
    except OperationFailure as e:
        if getattr(e, "code", None) != 85:  # Error => IndexOptionsConflict
            raise
    
    s.create_index([("email", ASCENDING), ("active", ASCENDING), ("started_at", ASCENDING)],
                   name="ix_sessions_email_active_time")
    s.create_index([("user_id", ASCENDING), ("active", ASCENDING)], name="ix_sessions_user_active")

def _migrate_legacy_password_if_needed(u: Dict[str, Any], plain_pwd: str) -> Dict[str, Any]:
    if isinstance(u.get("password"), dict):
        return u

    need_migration = False
    if isinstance(u.get("password"), str) and u["password"] == plain_pwd:
        need_migration = True
    if not need_migration and isinstance(u.get("passwordHash"), str) and u["passwordHash"] == plain_pwd:
        need_migration = True

    if need_migration:
        hashed = _hash_password(plain_pwd)
        _db().users.update_one({"_id": u["_id"]},
                               {"$set": {"password": hashed},
                                "$unset": {"passwordHash": ""}})
        u = _db().users.find_one({"_id": u["_id"]})
    return u


def login(email: str, password: str, role_override: Optional[str] = None) -> Dict[str, Any]:
    users_bootstrap()
    bootstrap_indexes()

    db = _db()
    u = db.users.find_one({"email": email.strip().lower(), "status": {"$in": ["active", "activo"]}})
    if not u:
        raise ValueError("Usuario inexistente o inactivo")

    u = _migrate_legacy_password_if_needed(u, password)

    pwd_info = u.get("password")
    if not isinstance(pwd_info, dict):
        raise ValueError("El usuario no tiene password configurado (migración requerida)")

    if not verify_password(password, pwd_info):
        raise ValueError("Credenciales inválidas")

    roles = list(u.get("roles", []))
    if role_override and role_override in roles:
        roles = [role_override]

    doc = db.counters.find_one_and_update(
    {"_id": "sessions"},
    {"$inc": {"seq": 1}},
    upsert=True,
    return_document=True
)
    sid = int(doc.get("seq", 1))

    sess = {
        "session_id": sid,
        "user_id": u.get("user_id"),
        "email": u["email"],
        "roles": roles,
        "active": True,
        "started_at": _now(),
        "ended_at": None,
    }
    db.sessions.insert_one(sess)

    rcli = _r()
    if rcli:
        try:
            rcli.setex(f"session:{sid}", SESSION_TTL_HOURS * 3600,
                       json.dumps({"user_id": sess["user_id"], "email": sess["email"], "roles": roles}))
        except Exception:
            pass

    return {k: v for k, v in sess.items() if k != "_id"}

def logout(email: str) -> int:
    """Cierra TODAS las sesiones activas del usuario (por email). Devuelve cuántas cerró."""
    bootstrap_indexes()
    db = _db()
    email = email.strip().lower()

    active_sessions = list(db.sessions.find({"email": email, "active": True}, {"session_id": 1}))
    if not active_sessions:
        return 0

    db.sessions.update_many({"email": email, "active": True},
                            {"$set": {"active": False, "ended_at": _now()}})

    rcli = _r()
    if rcli:
        for s in active_sessions:
            sid = s.get("session_id")
            if sid:
                try:
                    rcli.delete(f"session:{sid}")
                except Exception:
                    pass
    return len(active_sessions)

def get_active_sessions(user_email: Optional[str] = None) -> List[Dict[str, Any]]:
    """Lista sesiones activas (filtradas por email si se provee)."""
    q = {"active": True}
    if user_email:
        q["email"] = user_email.strip().lower()
    return list(_db().sessions.find(q, {"_id": 0}).sort("started_at", 1))

def session_has_permission(session_id: str, permission: str) -> bool:
    """
    Verifica si la sesión tiene el permiso indicado a partir de sus roles.
    Busca en Redis y cae a Mongo si no está.
    """
    roles: List[str] = []

    rcli = _r()
    if rcli:
        try:
            cached = rcli.get(f"session:{session_id}")
            if cached:
                payload = json.loads(cached.decode("utf-8"))
                roles = payload.get("roles", [])
        except Exception:
            roles = []

    if not roles:
        sess = _db().sessions.find_one({"session_id": session_id, "active": True})
        if not sess:
            return False
        roles = list(sess.get("roles", []))

    if not roles:
        return False

    perms: set = set()
    for rdoc in _db().roles.find({"name": {"$in": roles}}):
        perms.update(rdoc.get("permissions", []))
    return permission in perms
