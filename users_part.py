from __future__ import annotations
import os
import hashlib
import secrets
import datetime as dt
from typing import List, Dict, Any, Optional

from pymongo import MongoClient, ASCENDING, ReturnDocument
from pymongo.errors import OperationFailure


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "tp_poliglota")

def _db():
    return MongoClient(MONGO_URI)[MONGO_DB]

def _uid_from_email(email: str) -> str:
    base = email.strip().lower().encode("utf-8")
    return "usr_" + hashlib.sha1(base).hexdigest()[:12]

def _now() -> dt.datetime:
    return dt.datetime.utcnow()


def _hash_password(password: str, salt: Optional[str] = None, iterations: int = 120_000) -> Dict[str, str]:
    if not salt:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), iterations)
    return {"hash": dk.hex(), "salt": salt, "iterations": iterations}

def verify_password(password: str, stored: Dict[str, Any]) -> bool:
    calc = _hash_password(password, stored["salt"], stored.get("iterations", 120_000))
    return secrets.compare_digest(calc["hash"], stored["hash"])


def bootstrap_indexes():
    db = _db()
    u = db.users
    try:
        for idx in u.list_indexes():
            key = idx.get("key", {})
            if list(key.items()) == [("email", 1)] and (idx.get("name") != "ux_users_email" or not idx.get("unique", False)):
                try:
                    u.drop_index(idx["name"])
                except Exception:
                    pass
    except Exception:
        pass

    try:
        u.create_index([("email", ASCENDING)], unique=True, name="ux_users_email")
    except OperationFailure as e:
        if getattr(e, "code", None) != 85: # IndexOptionsConflict
            raise
    u.create_index([("status", ASCENDING), ("created_at", ASCENDING)], name="ix_users_status_created")


    r = db.roles
    try:
        for idx in r.list_indexes():
            key = idx.get("key", {})
            if list(key.items()) == [("name", 1)] and idx.get("name") != "ux_roles_name":
                try:
                    r.drop_index(idx["name"])
                except Exception:
                    pass
    except Exception:
        pass

    try:
        r.create_index([("name", ASCENDING)], unique=True, name="ux_roles_name")
    except OperationFailure as e:
        if getattr(e, "code", None) != 85:
            raise


# Roles
def create_role(name: str, permissions: List[str]) -> Dict[str, Any]:
    # Crea/actualiza un rol
    bootstrap_indexes()
    db = _db()
    doc = {"name": name, "permissions": sorted(set(permissions)), "created_at": _now()}
    existing = db.roles.find_one({"name": name})
    if existing:
        db.roles.update_one({"_id": existing["_id"]}, {"$set": {"permissions": doc["permissions"]}})
        existing["permissions"] = doc["permissions"]
        return existing
    db.roles.insert_one(doc)
    return doc

def list_roles() -> List[Dict[str, Any]]:
    return list(_db().roles.find({}, {"_id": 0}))


# Users
def create_user(full_name: str, email: str, password: str,
                roles: Optional[List[str]] = None, status: str = "active") -> Dict[str, Any]:
    
    #Crea o actualiza un usuario con upsert por email.
    #  Si el email ya existe: actualiza nombre/roles/estado; NO sobreescribe el password.
    #  Si no existe: crea con password hasheado.
    #Devuelve el documento sin el campo password.
    bootstrap_indexes()
    db = _db()

    email_n = email.strip().lower()
    uid = _uid_from_email(email_n)
    now = _now()
    pwd = _hash_password(password)

    set_on_insert = {
        "user_id": uid,
        "email": email_n,
        "password": pwd,
        "created_at": now,
    }
    set_always = {
        "full_name": full_name,
        "roles": sorted(set(roles or ["usuario"])),
        "status": status,
        "updated_at": now,
    }

    saved = db.users.find_one_and_update(
        {"email": email_n},
        {"$setOnInsert": set_on_insert, "$set": set_always},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    saved.pop("password", None)
    return saved

def assign_role(email: str, role: str) -> Dict[str, Any]:
    db = _db()
    updated = db.users.find_one_and_update(
        {"email": email.strip().lower()},
        {"$addToSet": {"roles": role}, "$set": {"updated_at": _now()}},
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        raise ValueError("Usuario no encontrado")
    updated.pop("password", None)
    return updated

def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    db = _db()
    email_n = email.strip().lower()
    u = db.users.find_one({"email": email_n})
    if not u:
        return None
    if "user_id" not in u or not u.get("user_id"):
        try:
            uid = _uid_from_email(email_n)
            u = db.users.find_one_and_update(
                {"_id": u["_id"]},
                {"$set": {"user_id": uid, "updated_at": _now()}},
                return_document=ReturnDocument.AFTER,
            )
        except Exception:
            u["user_id"] = _uid_from_email(email_n)
    u.pop("password", None)
    return u


# Seed roles básicos
def seed_basic_roles():
    #Crea roles default: usuario, tecnico, administrador
    create_role("usuario", [
        "read:own_reports", "request:process", "message:send", "message:read"
    ])
    create_role("tecnico", [
        "sensor:read", "sensor:write", "alert:manage",
        "read:all_reports", "message:send", "message:read"
    ])
    create_role("administrador", [
        "user:manage", "billing:manage", "read:all_reports",
        "process:manage", "group:manage", "message:send", "message:read"
    ])


if __name__ == "__main__":
    bootstrap_indexes()
    seed_basic_roles()
    print("users_part: índices OK y roles base creados (si no existían).")
