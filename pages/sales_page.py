import streamlit as st
from datetime import datetime
from firebase_config import db
from firebase_admin import firestore

from utils.helpers import now_iso, to_float
from services.firestore_queries import col_to_list, doc_get, doc_set


# ---------------------------
# Helpers
# ---------------------------

def write_stock_move(move: dict):
    move["created_at"] = now_iso()
    move["active"] = True
    db.collection("stock_moves").add(move)



# ---------------------------
# Customer special prices
# ---------------------------
def _load_customer_prices_map(customer_id: str, limit=500):
    """
    يرجع dict:
      product_id -> price
    """
    docs = db.collection("customer_prices") \
        .where("customer_id", "==", customer_id) \
        .limit(limit) \
        .stream()

    mp = {}
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        pid = x.get("product_id")
        if not pid:
            continue
        mp[pid] = float(to_float(x.get("price", 0.0)))
    return mp


# ---------------------------
# Page: Sales (Wholesale light)
# ---------------------------
def sales_page(go, user):
    st.markdown("<h2 style='text-align:center;'>🧾 مبيعات التوزيع</h2>", unsafe_allow_html=True)
    st.caption("اختيار عميل + سلة منتجات + نقدي/آجل → خصم مخزون + فاتورة + حركة sale + رصيد عميل (للآجل)")
    st.divider()

    top_left, _, _ = st.columns([1, 2, 1])
    with top_left:
        if st.button("⬅️ رجوع للوحة التحكم", key="back_to_dashboard_sales"):
            go("dashboard")

    # تحميل العملاء
    customers = col_to_list("customers", where_active=True)
    if not customers:
        st.warning("لا يوجد عملاء. أضف عميل أولًا من صفحة العملاء 👥.")
        return

    cust_map = {c.get("name", c["id"]): c["id"] for c in customers}
    cust_by_id = {c["id"]: c for c in customers}

    st.subheader("👤 بيانات العميل")
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        cust_name = st.selectbox("اختر العميل", options=[""] + list(cust_map.keys()), key="sale_customer_select")
    with c2:
        payment_type = st.selectbox("نوع الدفع", options=["cash", "credit"], index=0, key="sale_payment_type")
    with c3:
        if st.button("🔄 تحديث الأسعار الخاصة", use_container_width=True, key="reload_customer_prices_btn"):
            st.session_state.pop("customer_prices_map", None)
            st.session_state.pop("customer_prices_for", None)
            st.rerun()

    if not cust_name:
        st.info("اختر عميل للمتابعة.")
        return

    customer_id = cust_map[cust_name]
    customer = cust_by_id.get(customer_id, {})
    st.caption(f"الرصيد الحالي: **{to_float(customer.get('balance',0)):.2f}**")

    # ✅ حمّل الأسعار الخاصة مرة واحدة لكل عميل
    if st.session_state.get("customer_prices_for") != customer_id:
        st.session_state.customer_prices_map = _load_customer_prices_map(customer_id)
        st.session_state.customer_prices_for = customer_id

    customer_prices_map = st.session_state.get("customer_prices_map", {}) or {}
    if customer_prices_map:
        st.info(f"💰 يوجد أسعار خاصة لهذا العميل على {len(customer_prices_map)} منتج.")

    st.divider()

    # تحميل المنتجات (مرة واحدة)
    if "products_cache_for_sales" not in st.session_state:
        st.session_state.products_cache_for_sales = col_to_list("products", where_active=True)

    if st.button("🔄 تحديث المنتجات", key="refresh_products_cache_for_sales"):
        st.session_state.pop("products_cache_for_sales", None)
        st.rerun()

    products = st.session_state.get("products_cache_for_sales", [])
    if not products:
        st.info("لا يوجد منتجات. أضف منتجات أولًا من إدارة المستودع.")
        return

    prod_map = {p.get("name", p["id"]): p["id"] for p in products}
    prod_by_id = {p["id"]: p for p in products}

    # session cart
    if "sale_cart" not in st.session_state:
        st.session_state.sale_cart = []

    st.subheader("🛒 السلة")

    a1, a2, a3, a4 = st.columns([2, 1, 1, 1])
    with a1:
        prod_name = st.selectbox("المنتج", options=[""] + list(prod_map.keys()), key="sale_add_product")
    with a2:
        qty = st.number_input("الكمية (pcs)", min_value=0.0, step=1.0, value=0.0, key="sale_add_qty")
    with a3:
        quick = st.selectbox("سريع", options=[1, 5, 10], index=0, key="sale_quick_qty")
    with a4:
        if st.button("➕ إضافة", use_container_width=True, key="sale_add_btn"):
            if not prod_name:
                st.error("اختر منتج")
            elif qty <= 0:
                st.error("أدخل كمية أكبر من صفر")
            else:
                pid = prod_map[prod_name]
                p = prod_by_id.get(pid, {}) or {}

                base_price = to_float(p.get("price", 0.0))
                unit = p.get("sale_unit", "pcs")

                # ✅ السعر الخاص إن وجد
                special_price = customer_prices_map.get(pid, None)
                price = float(base_price if special_price is None else special_price)

                # دمج نفس المنتج بالسلة
                found = False
                for line in st.session_state.sale_cart:
                    if line["product_id"] == pid:
                        line["qty"] = to_float(line["qty"]) + float(qty)
                        # حدّث السعر لآخر قيمة (خاصة/عادية)
                        line["price"] = float(price)
                        line["price_source"] = "special" if special_price is not None else "base"
                        found = True
                        break

                if not found:
                    st.session_state.sale_cart.append({
                        "product_id": pid,
                        "product_name": p.get("name", prod_name),
                        "unit": unit,
                        "qty": float(qty),
                        "price": float(price),
                        "price_source": "special" if special_price is not None else "base",
                    })

                st.rerun()

    # زر إضافة سريعة (لو بدك)
    if prod_name:
        pid_preview = prod_map[prod_name]
        pprev = prod_by_id.get(pid_preview, {}) or {}
        base_price = to_float(pprev.get("price", 0.0))
        special_price = customer_prices_map.get(pid_preview, None)
        show_price = base_price if special_price is None else special_price
        st.caption(f"السعر المستخدم: **{float(show_price):.3f}** " + ("(سعر خاص)" if special_price is not None else "(سعر أساسي)"))

        if st.button(f"➕ إضافة سريع +{int(quick)}", key="sale_add_quick_btn"):
            pid = pid_preview
            p = pprev
            price = float(show_price)
            found = False
            for line in st.session_state.sale_cart:
                if line["product_id"] == pid:
                    line["qty"] = to_float(line["qty"]) + float(quick)
                    line["price"] = float(price)
                    line["price_source"] = "special" if special_price is not None else "base"
                    found = True
                    break
            if not found:
                st.session_state.sale_cart.append({
                    "product_id": pid,
                    "product_name": p.get("name", prod_name),
                    "unit": p.get("sale_unit", "pcs"),
                    "qty": float(quick),
                    "price": float(price),
                    "price_source": "special" if special_price is not None else "base",
                })
            st.rerun()

    if not st.session_state.sale_cart:
        st.info("السلة فارغة.")
        return

    # عرض السلة
    cart_rows = []
    for line in st.session_state.sale_cart:
        cart_rows.append({
            "product_id": line["product_id"],
            "المنتج": line.get("product_name", ""),
            "الوحدة": line.get("unit", "pcs"),
            "الكمية": to_float(line.get("qty")),
            "السعر": to_float(line.get("price")),
            "مصدر السعر": "خاص" if line.get("price_source") == "special" else "أساسي",
            "الإجمالي": round(to_float(line.get("qty")) * to_float(line.get("price")), 3),
        })

    edited = st.data_editor(
        cart_rows,
        use_container_width=True,
        hide_index=True,
        disabled=["product_id", "المنتج", "الوحدة", "مصدر السعر", "الإجمالي"],
        column_config={
            "الكمية": st.column_config.NumberColumn("الكمية", step=1.0),
            "السعر": st.column_config.NumberColumn("السعر", step=0.05),
        },
        key="sale_cart_editor"
    )

    # تحديث السلة من التعديلات
    new_cart = []
    for r in edited:
        new_cart.append({
            "product_id": r["product_id"],
            "product_name": r["المنتج"],
            "unit": r["الوحدة"],
            "qty": float(to_float(r.get("الكمية"))),
            "price": float(to_float(r.get("السعر"))),
            "price_source": "manual",  # المستخدم عدّل السعر يدويًا داخل السلة
        })
    st.session_state.sale_cart = new_cart

    total_qty = sum(to_float(l["qty"]) for l in st.session_state.sale_cart)
    total_amount = sum(to_float(l["qty"]) * to_float(l["price"]) for l in st.session_state.sale_cart)

    m1, m2, m3 = st.columns(3)
    m1.metric("إجمالي الكمية", f"{total_qty:.2f}")
    m2.metric("إجمالي المبلغ", f"{total_amount:.2f}")
    m3.metric("الدفع", "نقدي" if payment_type == "cash" else "آجل")

    b1, b2 = st.columns(2)
    with b1:
        if st.button("🗑️ تفريغ السلة", use_container_width=True, key="sale_clear_cart"):
            st.session_state.sale_cart = []
            st.rerun()

    with b2:
        if st.button("✅ اعتماد البيع", use_container_width=True, key="sale_submit"):
            try:
                sale_id, total = _commit_sale_transaction_wholesale(
                    cart_lines=st.session_state.sale_cart,
                    user=user,
                    customer_id=customer_id,
                    customer_name=cust_name,
                    payment_type=payment_type
                )

                # stock_moves
                for line in st.session_state.sale_cart:
                    write_stock_move({
                        "type": "sale",
                        "ref_type": "sale",
                        "ref_id": sale_id,
                        "item_type": "product",
                        "item_id": line["product_id"],
                        "item_name": line.get("product_name", ""),
                        "qty_delta": -float(to_float(line.get("qty"))),
                        "unit": line.get("unit", "pcs"),
                        "note": f"بيع ({'نقدي' if payment_type=='cash' else 'آجل'})",
                        "created_by": user.get("username", ""),
                        "customer_id": customer_id,
                        "customer_name": cust_name,
                    })

                st.success(f"تم اعتماد البيع ✅ (ID: {sale_id}) | الإجمالي: {total:.2f}")
                st.session_state.sale_cart = []
                st.rerun()

            except Exception as e:
                st.error(f"فشل اعتماد البيع: {e}")

    st.divider()
    _sales_history_ui()


# ---------------------------
# Transaction: sale + stock - customer balance (if credit)
# ---------------------------
def _commit_sale_transaction_wholesale(cart_lines, user, customer_id, customer_name, payment_type):
    lines = []
    for l in cart_lines:
        qty = to_float(l.get("qty"))
        if qty <= 0:
            continue
        lines.append({
            "product_id": l["product_id"],
            "product_name": l.get("product_name", ""),
            "unit": l.get("unit", "pcs"),
            "qty": float(qty),
            "price": float(to_float(l.get("price"))),
        })

    if not lines:
        raise ValueError("السلة فارغة أو الكميات غير صحيحة.")

    @firestore.transactional
    def tx_do(transaction):
        # اقرأ المنتجات وتحقق المخزون
        prod_refs, prod_snaps = [], []
        for line in lines:
            ref = db.collection("products").document(line["product_id"])
            snap = ref.get(transaction=transaction)
            if not snap.exists:
                raise ValueError(f"منتج غير موجود: {line.get('product_name','')}")
            prod_refs.append(ref)
            prod_snaps.append(snap)

        for line, snap in zip(lines, prod_snaps):
            cur = to_float((snap.to_dict() or {}).get("qty_on_hand", 0))
            req = float(line["qty"])
            if cur < req:
                pname = (snap.to_dict() or {}).get("name", line.get("product_name", ""))
                raise ValueError(f"المخزون غير كافٍ للمنتج: {pname} (المطلوب {req}, المتوفر {cur})")

        # خصم مخزون المنتجات
        for line, ref, snap in zip(lines, prod_refs, prod_snaps):
            cur = to_float((snap.to_dict() or {}).get("qty_on_hand", 0))
            transaction.update(ref, {"qty_on_hand": cur - float(line["qty"]), "updated_at": now_iso()})

        # حساب الإجمالي
        total = sum(float(l["qty"]) * float(l["price"]) for l in lines)

        # إذا آجل: زِد رصيد العميل
        if payment_type == "credit":
            cust_ref = db.collection("customers").document(customer_id)
            cust_snap = cust_ref.get(transaction=transaction)
            if not cust_snap.exists:
                raise ValueError("العميل غير موجود.")
            cur_bal = to_float((cust_snap.to_dict() or {}).get("balance", 0))
            transaction.update(cust_ref, {"balance": cur_bal + float(total), "updated_at": now_iso()})

        # إنشاء فاتورة
        sale_ref = db.collection("sales").document()
        transaction.set(sale_ref, {
            "status": "posted",
            "active": True,
            "created_at": now_iso(),
            "created_by": user.get("username", ""),
            "customer_id": customer_id,
            "customer_name": customer_name,
            "payment_type": payment_type,   # cash / credit
            "lines": [
                {
                    "product_id": l["product_id"],
                    "product_name": l["product_name"],
                    "unit": l["unit"],
                    "qty": l["qty"],
                    "price": l["price"],
                    "line_total": round(float(l["qty"]) * float(l["price"]), 3),
                } for l in lines
            ],
            "total": round(float(total), 3),
        })

        return sale_ref.id, float(total)

    return tx_do(db.transaction())


# ---------------------------
# Sales History (simple)
# ---------------------------
def _sales_history_ui():
    st.subheader("📜 سجل المبيعات (آخر 30)")

    try:
        docs = db.collection("sales").order_by("created_at", direction=firestore.Query.DESCENDING).limit(50).stream()
    except Exception:
        docs = db.collection("sales").limit(50).stream()

    rows = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        rows.append({
            "id": d.id,
            "created_at": x.get("created_at", ""),
            "customer": x.get("customer_name", ""),
            "pay": x.get("payment_type", ""),
            "total": to_float(x.get("total", 0)),
            "status": x.get("status", ""),
        })

    rows = rows[:30]
    if not rows:
        st.info("لا يوجد مبيعات بعد.")
        return

    st.dataframe(rows, use_container_width=True, hide_index=True)
