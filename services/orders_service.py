from firebase_config import db
from firebase_admin import firestore

from utils.helpers import now_iso, to_float
from services.firestore_queries import doc_get


def write_stock_moves_batch(moves: list[dict]):
    if not moves:
        return

    batch = db.batch()
    ts = now_iso()

    for move in moves:
        move["created_at"] = ts
        move["active"] = True
        ref = db.collection("stock_moves").document()
        batch.set(ref, move)

    batch.commit()


def cancel_prepared_sale(sid: str, user: dict):
    @firestore.transactional
    def tx_cancel(transaction):
        ts = now_iso()

        sale_ref = db.collection("sales").document(sid)
        sale_snap = sale_ref.get(transaction=transaction)

        if not sale_snap.exists:
            raise ValueError("الفاتورة غير موجودة")

        sale = sale_snap.to_dict() or {}

        if sale.get("active") is not True:
            raise ValueError("الفاتورة غير فعالة")

        if sale.get("status") != "prepared":
            raise ValueError("لا يمكن إلغاء إلا الطلبات المحضّرة فقط")

        items = sale.get("items", []) or []

        prod_rows = []

        for it in items:
            pid = it.get("product_id")
            qty = float(to_float(it.get("qty", 0)))

            if not pid or qty <= 0:
                continue

            prod_ref = db.collection("products").document(pid)
            prod_snap = prod_ref.get(transaction=transaction)

            if not prod_snap.exists:
                raise ValueError(f"المنتج غير موجود: {it.get('product_name', '')}")

            prod_data = prod_snap.to_dict() or {}
            cur_qty = float(to_float(prod_data.get("qty_on_hand", 0)))

            prod_rows.append({
                "ref": prod_ref,
                "qty": qty,
                "cur_qty": cur_qty,
            })

        for row in prod_rows:
            transaction.update(row["ref"], {
                "qty_on_hand": row["cur_qty"] + row["qty"],
                "updated_at": ts,
            })

        transaction.update(sale_ref, {
            "status": "cancelled",
            "updated_at": ts,
            "cancelled_at": ts,
            "cancelled_by": user.get("username", ""),
            "cancel_reason": "إلغاء طلب محضّر قبل التسليم",
            "stock_returned": True,
        })

    tx_cancel(db.transaction())

    sale = doc_get("sales", sid) or {}

    moves = []

    for it in (sale.get("items", []) or []):
        moves.append({
            "type": "sale_cancel",
            "ref_type": "sale_cancelled",
            "ref_id": sid,
            "item_type": "product",
            "item_id": it.get("product_id", ""),
            "item_name": it.get("product_name", ""),
            "qty_delta": float(to_float(it.get("qty", 0))),
            "unit": (it.get("unit") or "pcs"),
            "note": "إرجاع مخزون بسبب إلغاء طلب محضّر قبل التسليم",
            "created_by": user.get("username", ""),
        })

    write_stock_moves_batch(moves)