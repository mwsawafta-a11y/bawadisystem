import streamlit as st
from datetime import datetime
from firebase_config import db
from firebase_admin import firestore

from utils.helpers import now_iso, to_float
from services.firestore_queries import col_to_list


def write_stock_move(move: dict):
    move["created_at"] = now_iso()
    move["active"] = True
    db.collection("stock_moves").add(move)


# ---------------------------
# Page
# ---------------------------
def payments_page(go, user):
    st.markdown("<h2 style='text-align:center;'>💰 التحصيل</h2>", unsafe_allow_html=True)
    st.caption("تسجيل دفعات العملاء (تُخفض رصيد العميل) مع سجل عمليات.")
    st.divider()

    top_left, _, _ = st.columns([1, 2, 1])
    with top_left:
        if st.button("⬅️ رجوع للوحة التحكم", key="back_to_dashboard_payments"):
            go("dashboard")

    customers = col_to_list("customers", where_active=True)
    if not customers:
        st.warning("لا يوجد عملاء. أضف عميل أولًا من صفحة العملاء 👥.")
        return

    cust_map = {c.get("name", c["id"]): c["id"] for c in customers if c.get("name") or c.get("id")}
    cust_by_id = {c["id"]: c for c in customers}

    st.subheader("🧾 تسجيل تحصيل")
    c1, c2 = st.columns([2, 1])
    with c1:
        cust_name = st.selectbox("اختر العميل", options=[""] + list(cust_map.keys()), key="pay_customer_select")
    with c2:
        pay_date = st.date_input("التاريخ", key="pay_date", value=datetime.utcnow().date())

    if not cust_name:
        st.info("اختر عميل للمتابعة.")
        return

    customer_id = cust_map[cust_name]
    customer = cust_by_id.get(customer_id, {})
    current_balance = to_float(customer.get("balance", 0.0))
    st.caption(f"الرصيد الحالي على العميل: **{current_balance:.2f}**")

    amount = st.number_input("مبلغ التحصيل", min_value=0.0, step=1.0, value=0.0, key="pay_amount")
    note = st.text_input("ملاحظة (اختياري)", placeholder="مثال: دفعة نقدية / تحويل / ...", key="pay_note")

    prevent_negative = st.checkbox("منع أن يصبح الرصيد سالب", value=True, key="pay_prevent_negative")

    if st.button("✅ حفظ التحصيل", use_container_width=True, key="pay_submit"):
        if amount <= 0:
            st.error("أدخل مبلغ أكبر من صفر")
            return

        try:
            pay_id, before_bal, after_bal, created_at = _commit_payment_transaction(
                customer_id=customer_id,
                customer_name=cust_name,
                amount=float(amount),
                pay_date=str(pay_date),
                note=note.strip(),
                created_by=(user.get("username") or ""),
                prevent_negative=prevent_negative
            )

            write_stock_move({
                "type": "collection",
                "ref_type": "payment",
                "ref_id": pay_id,
                "item_type": "customer",
                "item_id": customer_id,
                "item_name": cust_name,
                "qty_delta": 0,
                "unit": "",
                "note": f"تحصيل {float(amount):.2f} | رصيد قبل: {before_bal:.2f} بعد: {after_bal:.2f} | {note.strip()}",
                "created_by": (user.get("username") or ""),
                "customer_id": customer_id,
                "customer_name": cust_name,
                "amount": float(amount),
                "balance_before": float(before_bal),
                "balance_after": float(after_bal),
                "created_at": created_at,
            })

            st.success(f"تم تسجيل التحصيل ✅ (ID: {pay_id}) | الرصيد الآن: {after_bal:.2f}")
            st.rerun()

        except Exception as e:
            st.error(f"فشل تسجيل التحصيل: {e}")

    st.divider()
    st.subheader("📜 سجل التحصيل (آخر 30)")

    try:
        docs = db.collection("payments") \
            .order_by("created_at", direction=firestore.Query.DESCENDING) \
            .limit(50).stream()
        docs_list = list(docs)
    except Exception:
        docs_list = list(db.collection("payments").limit(80).stream())

    rows = []
    for d in docs_list:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        rows.append({
            "id": d.id,
            "created_at": x.get("created_at", ""),
            "date": x.get("date", ""),
            "customer": x.get("customer_name", ""),
            "amount": to_float(x.get("amount", 0)),
            "by": x.get("created_by", ""),
            "note": x.get("note", ""),
        })

    rows.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    rows = rows[:30]

    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("لا يوجد تحصيل بعد.")


# ---------------------------
# Transaction
# ---------------------------
def _commit_payment_transaction(customer_id, customer_name, amount, pay_date, note, created_by, prevent_negative=True):
    created_at = now_iso()

    @firestore.transactional
    def tx_do(transaction):
        cust_ref = db.collection("customers").document(customer_id)

        cust_snap = cust_ref.get(transaction=transaction)
        if not cust_snap.exists:
            raise ValueError("العميل غير موجود.")

        cur_bal = to_float((cust_snap.to_dict() or {}).get("balance", 0.0))
        new_bal = cur_bal - float(amount)

        if prevent_negative and new_bal < 0:
            raise ValueError(f"المبلغ أكبر من الرصيد. الرصيد الحالي {cur_bal:.2f}")

        transaction.update(cust_ref, {"balance": new_bal, "updated_at": created_at})

        pay_ref = db.collection("payments").document()
        transaction.set(pay_ref, {
            "active": True,
            "status": "posted",
            "created_at": created_at,
            "created_by": created_by,
            "customer_id": customer_id,
            "customer_name": customer_name,
            "date": pay_date,
            "amount": float(amount),
            "note": note,
            "balance_before": float(cur_bal),
            "balance_after": float(new_bal),
        })

        return pay_ref.id, cur_bal, new_bal, created_at

    return tx_do(db.transaction())