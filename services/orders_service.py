from firebase_config import db
from firebase_admin import firestore

from utils.helpers import now_iso, to_float


def _safe_str(x):
    return str(x or "").strip()


def _stock_move_doc_id(move: dict, idx: int) -> str:
    """
    معرّف ثابت لمنع تكرار تسجيل نفس حركة المخزون إذا تكرر التنفيذ.
    """
    return "sm__{ref_type}__{ref_id}__{item_type}__{item_id}__{move_type}__{idx}".format(
        ref_type=_safe_str(move.get("ref_type")),
        ref_id=_safe_str(move.get("ref_id")),
        item_type=_safe_str(move.get("item_type")),
        item_id=_safe_str(move.get("item_id")),
        move_type=_safe_str(move.get("type")),
        idx=int(idx),
    )


def write_stock_moves_batch(moves: list[dict]):
    if not moves:
        return

    batch = db.batch()
    ts = now_iso()

    for idx, move in enumerate(moves):
        payload = dict(move or {})
        payload["created_at"] = payload.get("created_at") or ts
        payload["active"] = True

        ref = db.collection("stock_moves").document(_stock_move_doc_id(payload, idx))
        batch.set(ref, payload, merge=True)

    batch.commit()


def cancel_prepared_sale(sid: str, user: dict):
    sale_items_for_moves = []

    @firestore.transactional
    def tx_cancel(transaction):
        nonlocal sale_items_for_moves

        ts = now_iso()

        sale_ref = db.collection("sales").document(sid)
        sale_snap = sale_ref.get(transaction=transaction)

        if not sale_snap.exists:
            raise ValueError("الفاتورة غير موجودة")

        sale = sale_snap.to_dict() or {}

        if sale.get("active") is not True:
            raise ValueError("الفاتورة غير فعالة")

        current_status = sale.get("status")

        # حماية من التكرار
        if current_status == "cancelled":
            sale_items_for_moves = sale.get("items", []) or []
            return

        if current_status != "prepared":
            raise ValueError("لا يمكن إلغاء إلا الطلبات المحضّرة فقط")

        items = sale.get("items", []) or []
        sale_items_for_moves = items

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

    moves = []

    for it in (sale_items_for_moves or []):
        qty = float(to_float(it.get("qty", 0)))
        pid = it.get("product_id", "")

        if not pid or qty <= 0:
            continue

        moves.append({
            "type": "sale_cancel",
            "ref_type": "sale_cancelled",
            "ref_id": sid,
            "item_type": "product",
            "item_id": pid,
            "item_name": it.get("product_name", ""),
            "qty_delta": qty,
            "unit": (it.get("unit") or "pcs"),
            "note": "إرجاع مخزون بسبب إلغاء طلب محضّر قبل التسليم",
            "created_by": user.get("username", ""),
        })

    write_stock_moves_batch(moves)