from firebase_config import db
from utils.helpers import now_iso

def col_to_list(collection_name: str, where_active=True, limit=None):
    ref = db.collection(collection_name)
    if where_active:
        ref = ref.where("active", "==", True)
    if limit:
        ref = ref.limit(int(limit))
    docs = ref.stream()
    out = []
    for d in docs:
        item = d.to_dict() or {}
        item["id"] = d.id
        out.append(item)
    return out

def doc_get(collection: str, doc_id: str):
    d = db.collection(collection).document(doc_id).get()
    return d.to_dict() if d.exists else None

def doc_set(collection: str, doc_id: str, data: dict, merge=True):
    db.collection(collection).document(doc_id).set(data, merge=merge)

def doc_soft_delete(collection: str, doc_id: str):
    db.collection(collection).document(doc_id).set(
        {"active": False, "updated_at": now_iso()},
        merge=True
    )