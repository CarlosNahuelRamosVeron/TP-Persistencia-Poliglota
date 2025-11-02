from __future__ import annotations
import os
import datetime as dt
from typing import List, Dict, Any

from pymongo import MongoClient, ASCENDING, ReturnDocument

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "tp_poliglota")


def _db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


def _next_id(name: str) -> int:
    db = _db()
    counter = db.counters.find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return counter["seq"]


def bootstrap_indexes():
    db = _db()
    db.groups.create_index([("group_id", ASCENDING)], unique=True, name="ux_groups_id")
    db.groups.create_index([("name", ASCENDING)], name="ix_groups_name")
    db.messages.create_index([("message_id", ASCENDING)], unique=True, name="ux_messages_id")
    db.messages.create_index([("to_user_id", ASCENDING), ("created_at", ASCENDING)], name="ix_msg_user_time")
    db.messages.create_index([("group_id", ASCENDING), ("created_at", ASCENDING)], name="ix_msg_group_time")

# Grupos
def create_group(name: str, member_ids: List[str]) -> Dict[str, Any]:
    bootstrap_indexes()
    db = _db()
    gid = _next_id("groups")
    doc = {
        "group_id": gid,
        "name": name,
        "member_ids": sorted(set(member_ids)),
        "created_at": dt.datetime.utcnow()
    }
    db.groups.insert_one(doc)
    return doc

# Mensajes
def send_private_message(from_user_id: str, to_user_id: str, content: str) -> Dict[str, Any]:
    bootstrap_indexes()
    db = _db()
    mid = _next_id("messages")
    msg = {
        "message_id": mid,
        "type": "private",
        "from_user_id": from_user_id,
        "to_user_id": to_user_id,
        "group_id": None,
        "content": content,
        "created_at": dt.datetime.utcnow(),
        "read_by": []
    }
    db.messages.insert_one(msg)
    return msg

def send_group_message(from_user_id: str, group_id: int, content: str) -> Dict[str, Any]:
    bootstrap_indexes()
    db = _db()
    grp = db.groups.find_one({"group_id": group_id})
    if not grp:
        raise ValueError("Grupo inexistente")
    mid = _next_id("messages")
    msg = {
        "message_id": mid,
        "type": "groupal",
        "from_user_id": from_user_id,
        "to_user_id": None,
        "group_id": group_id,
        "content": content,
        "created_at": dt.datetime.utcnow(),
        "read_by": []
    }
    db.messages.insert_one(msg)
    return msg

# Lectura y bandeja
def list_inbox(user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    db = _db()
    groups = [g["group_id"] for g in db.groups.find({"member_ids": user_id}, {"group_id": 1})]
    q = {"$or": [{"to_user_id": user_id}, {"group_id": {"$in": groups}}]}
    cur = db.messages.find(q, {"_id": 0}).sort([("created_at", 1)]).limit(limit)
    return list(cur)

def mark_as_read(user_id: str, message_id: int) -> bool:
    db = _db()
    res = db.messages.update_one({"message_id": message_id}, {"$addToSet": {"read_by": user_id}})
    return res.modified_count > 0

if __name__ == "__main__":
    bootstrap_indexes()
    print("messaging_part: índices OK (autoincremental).")
