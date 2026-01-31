import streamlit as st
from datetime import datetime, timezone, timedelta
from firebase_config import db
from firebase_admin import firestore
from typing import Optional, cast
from google.cloud.firestore_v1 import DocumentSnapshot
import streamlit.components.v1 as components


# ---------------------------
# Helpers
# ---------------------------
def now_iso():
    # ✅ توقيت الأردن
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).isoformat()

def to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def col_to_list(collection_name: str, where_active=True):
    ref = db.collection(collection_name)
    if where_active:
        ref = ref.where("active", "==", True)
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
        {"active": False, "updated_at": now_iso()}, merge=True
    )
def add_collection(customer: dict, amount: float, user: dict, note: str = "", status: str = "posted"):
    customer = customer or {}
    cid = customer.get("id") or ""
    cname = customer.get("name") or cid

    doc_id = f"col__{cid}__{int(datetime.now().timestamp()*1000)}"
    doc_set("collections", doc_id, {
        "customer_id": cid,
        "customer_name": cname,
        "amount": float(amount),
        "note": (note or "").strip(),
        "status": status,  # posted / done
        "active": True,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "created_by": user.get("username", ""),
    }, merge=True)
    return doc_id
    

def _safe_created_at(x: dict):
    return x.get("created_at") or ""


# ---------------------------
# Statement queries (no composite index)
# ---------------------------
def _get_customer_credit_sales(customer_id: str, limit=300):
    docs = db.collection("sales").where("customer_id", "==", customer_id).limit(limit).stream()
    out = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        if x.get("status") not in ["posted", "done"]:
            continue
        if x.get("payment_type") != "credit":
            continue
        out.append({"id": d.id, **x})
    return out

def _get_customer_cash_sales(customer_id: str, limit=300):
    docs = db.collection("sales").where("customer_id", "==", customer_id).limit(limit).stream()
    out = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        if x.get("status") not in ["posted", "done"]:
            continue
        if x.get("payment_type") != "cash":
            continue
        out.append({"id": d.id, **x})
    return out


def _get_customer_collections(customer_id: str, limit=300):
    docs = db.collection("collections").where("customer_id", "==", customer_id).limit(limit).stream()
    out = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        if x.get("status") not in ["posted", "done"]:
            continue
        out.append({"id": d.id, **x})
    return out

def _get_customer_credit_returns(customer_id: str, limit=300):
    docs = db.collection("returns").where("customer_id", "==", customer_id).limit(limit).stream()
    out = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        if x.get("status") not in ["posted", "done"]:
            continue
        if x.get("settlement") != "credit_note":
            continue
        out.append({"id": d.id, **x})
    return out

def _build_statement(customer: dict):
    cid = customer["id"]
    opening = to_float(customer.get("opening_balance", 0))
    if "opening_balance" not in customer and "balance" in customer:
        opening = to_float(customer.get("balance", 0))

    sales_credit = _get_customer_credit_sales(cid)
    sales_cash = _get_customer_cash_sales(cid)
    cols = _get_customer_collections(cid)
    rets = _get_customer_credit_returns(cid)

    moves = []
    moves.append({
        "created_at": customer.get("created_at", ""),
        "type": "opening",
        "ref": "opening_balance",
        "delta": float(opening),     # أثر على الرصيد
        "net": float(opening),
        "paid": 0.0,
        "unpaid": 0.0,
        "extra": 0.0,
        "note": "دين سابق / رصيد افتتاحي",
    })

    # ✅ مبيعات ذمم (آجل): أثر الرصيد = +net
    for s in sales_credit:
        net = float(to_float(s.get("net", s.get("total", 0))))
        moves.append({
            "created_at": _safe_created_at(s),
            "type": "sale_credit",
            "ref": f"SALE:{s.get('id','')}",
            "delta": +net,
            "net": net,
            "paid": 0.0,
            "unpaid": net,
            "extra": 0.0,
            "note": "فاتورة ذمم (آجل)",
        })

    # ✅ مبيعات نقدي: تظهر بالحركات + أثر الرصيد = unpaid_debt - extra_credit
    for s in sales_cash:
        net = float(to_float(s.get("net", s.get("total", 0))))
        paid = float(to_float(s.get("amount_paid", 0)))
        unpaid = float(to_float(s.get("unpaid_debt", 0)))
        extra = float(to_float(s.get("extra_credit", 0)))

        delta = float(unpaid) - float(extra)  # هذا اللي يأثر على الرصيد

        note = "فاتورة نقدي"
        if unpaid > 0:
            note = "فاتورة نقدي (دفع جزئي + ذمم متبقي)"
        elif extra > 0:
            note = "فاتورة نقدي (زيادة كرصد للعميل)"

        moves.append({
            "created_at": _safe_created_at(s),
            "type": "sale_cash",
            "ref": f"SALE:{s.get('id','')}",
            "delta": delta,
            "net": net,
            "paid": paid,
            "unpaid": unpaid,
            "extra": extra,
            "note": note,
        })

    # ✅ التحصيلات: تقلل الرصيد (سداد)
    for c in cols:
        amt = float(to_float(c.get("amount", 0)))
        moves.append({
            "created_at": _safe_created_at(c),
            "type": "collection",
            "ref": f"COL:{c.get('id','')}",
            "delta": -amt,
            "net": 0.0,
            "paid": amt,
            "unpaid": 0.0,
            "extra": 0.0,
            "note": "تحصيل / سند قبض",
        })

    # ✅ المرتجعات (خصم دين)
    for r in rets:
        tot = float(to_float(r.get("total", 0)))
        moves.append({
            "created_at": _safe_created_at(r),
            "type": "return_credit",
            "ref": f"RET:{r.get('id','')}",
            "delta": -tot,
            "net": 0.0,
            "paid": 0.0,
            "unpaid": 0.0,
            "extra": 0.0,
            "note": "مرتجع (خصم دين)",
        })

    moves.sort(key=lambda m: m.get("created_at") or "")

    running = 0.0
    rows = []
    for m in moves:
        running += float(to_float(m.get("delta", 0.0)))

        rows.append({
            "التاريخ": (m.get("created_at", "") or "")[:19].replace("T", " "),
            "النوع": m.get("note", ""),
            "المرجع": m.get("ref", ""),
            "الصافي": round(float(to_float(m.get("net", 0.0))), 3),
            "المدفوع": round(float(to_float(m.get("paid", 0.0))), 3),
            "متبقي ذمم": round(float(to_float(m.get("unpaid", 0.0))), 3),
            "زيادة كرصد": round(float(to_float(m.get("extra", 0.0))), 3),
            "أثر على الرصيد": round(float(to_float(m.get("delta", 0.0))), 3),
            "الرصيد بعد العملية": round(float(running), 3),
        })

    # نرجع نفس المخرجات + نضيف cash_sales بدون ما نكسر شيء
    return rows, running, sales_credit, sales_cash, cols, rets

# ---------------------------
# Customer prices (light)
# ---------------------------
def _get_customer_prices_map(customer_id: str, limit=300):
    docs = (
        db.collection("customer_prices")
        .where("customer_id", "==", customer_id)
        .limit(limit)
        .stream()
    )
    out = {}
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        pid = x.get("product_id")
        if pid:
            out[pid] = {"id": d.id, **x}
    return out



def _money(x):
    try:
        return f"{float(x):.3f}"
    except Exception:
        return "0.000"

def build_customer_full_statement_html(customer: dict, rows: list, final_balance: float, company_name="نظام المخبز", paper="80mm"):
    customer = customer or {}
    cust_name = customer.get("name") or customer.get("id") or "—"
    phone = customer.get("phone") or ""
    dt = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

    width_css = "800px" if paper == "a4" else "280px"
    font_css = "14px" if paper == "a4" else "12px"

    # اعرض آخر 80 سطر بالطباعة عشان ما تصير طويلة
    rows = rows[-80:] if rows else []

    body_rows = ""
    for r in rows:
        body_rows += f"""
        <tr>
          <td>{r.get("التاريخ","")}</td>
          <td>{r.get("النوع","")}</td>
          <td>{r.get("المرجع","")}</td>
          <td>{_money(r.get("الصافي",0))}</td>
          <td>{_money(r.get("المدفوع",0))}</td>
          <td>{_money(r.get("متبقي ذمم",0))}</td>
          <td>{_money(r.get("زيادة كرصد",0))}</td>
          <td>{_money(r.get("أثر على الرصيد",0))}</td>
          <td><b>{_money(r.get("الرصيد بعد العملية",0))}</b></td>
        </tr>
        """

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Customer Statement</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; }}
  .wrap {{ width: {width_css}; margin: 0 auto; padding: 12px; font-size: {font_css}; }}
  .center {{ text-align: center; }}
  .muted {{ color: #666; }}
  hr {{ border: none; border-top: 1px dashed #999; margin: 10px 0; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 6px 3px; border-bottom: 1px solid #eee; text-align: right; vertical-align: top; }}
  th {{ border-bottom: 1px solid #ddd; }}
  .sumrow {{ display: flex; justify-content: space-between; margin-top: 6px; }}
  .btnbar {{ margin: 10px 0 0 0; display: flex; gap: 8px; }}
  button {{ padding: 10px 12px; cursor: pointer; width: 100%; }}
  @media print {{
    .btnbar {{ display: none; }}
    .wrap {{ width: 100%; }}
  }}
</style>
</head>
<body>
  <div class="wrap">
    <div class="center">
      <div style="font-size:18px;font-weight:700;">{company_name}</div>
      <div style="margin-top:6px;font-weight:700;">كشف حساب عميل (نقدي + ذمم)</div>
    </div>

    <hr/>

    <div class="sumrow"><span>العميل:</span><span><b>{cust_name}</b></span></div>
    {f"<div class='sumrow'><span>هاتف:</span><span>{phone}</span></div>" if phone else ""}
    <div class="sumrow"><span>الرصيد الحالي:</span><span><b>{_money(final_balance)}</b></span></div>
    <div class="sumrow"><span>تاريخ الطباعة:</span><span>{dt}</span></div>

    <hr/>

    <table>
      <thead>
        <tr>
          <th>تاريخ</th>
          <th>نوع</th>
          <th>مرجع</th>
          <th>صافي</th>
          <th>مدفوع</th>
          <th>متبقي</th>
          <th>زيادة</th>
          <th>أثر</th>
          <th>الرصيد</th>
        </tr>
      </thead>
      <tbody>
        {body_rows if body_rows else "<tr><td colspan='9' class='muted'>لا توجد حركات.</td></tr>"}
      </tbody>
    </table>

    <div class="btnbar">
      <button onclick="window.print()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html

def show_print_html(html: str, height=820):
    components.html(html, height=height, scrolling=True)

# ---------------------------
# Page: Customers
# ---------------------------
def customers_page(go, user):
    st.markdown("<h2 style='text-align:center;'>👥 العملاء</h2>", unsafe_allow_html=True)
    st.caption("إضافة عميل + تعديل معلوماته + تعديل الأسعار الخاصة + حذف + كشف حساب (خفيف وسريع)")
    st.divider()

    top_left, _, _ = st.columns([1, 2, 1])
    with top_left:
        if st.button("⬅️ رجوع للوحة التحكم", key="back_to_dashboard_customers"):
            go("dashboard")

    # ✅ كاش المنتجات (مرة واحدة) عشان نستخدمها في أسعار العملاء بدون بطء
    if "products_cache_for_customer_prices" not in st.session_state:
        st.session_state.products_cache_for_customer_prices = col_to_list("products", where_active=True)

    if st.button("🔄 تحديث المنتجات (أسعار العملاء)", key="refresh_products_cache_for_customer_prices"):
        st.session_state.pop("products_cache_for_customer_prices", None)
        st.rerun()

    products_cache = st.session_state.get("products_cache_for_customer_prices", []) or []
    products_cache = sorted(products_cache, key=lambda x: (x.get("name") or ""))

    # ✅ هذه هي نفس منتجات المستودع (اللي بتنضاف من inventory_page)
    PRODUCTS_FOR_PRICES = [
        {
            "id": p["id"],
            "name": p.get("name", p["id"]),
            "base_price": to_float(p.get("price", 0.0)),
        }
        for p in products_cache
        if (p.get("active") is True)
    ]

    tabs = st.tabs(["👥 إدارة العملاء", "📊 كشف حساب العميل"])

    # ---------------------------
    # Tab 1: Manage customers
    # ---------------------------
    with tabs[0]:
        st.subheader("👥 إدارة العملاء")

        # ===========================
        # ✅ Add customer
        # ===========================
        with st.expander("➕ إضافة عميل", expanded=False):
            st.markdown("### 💰 أسعار خاصة عند الإضافة (اختياري)")
            st.caption("فعّل الخيار لتظهر المنتجات فوراً (نفس منتجات المستودع). اترك السعر فارغ = يستخدم السعر العام.")
            st.checkbox("تحديد أسعار خاصة الآن", value=False, key="add_cust_enable_special")

            add_special_rows = []
            if st.session_state.get("add_cust_enable_special"):
                if not PRODUCTS_FOR_PRICES:
                    st.warning("لا يوجد منتجات. أضف منتجات أولاً من صفحة المستودع.")
                else:
                    st.divider()
                    for i in range(0, len(PRODUCTS_FOR_PRICES), 2):
                        cols = st.columns(2, gap="large")
                        pair = PRODUCTS_FOR_PRICES[i:i+2]

                        for j, p in enumerate(pair):
                            with cols[j]:
                                st.markdown(f"**🧾 {p['name']}**")
                                st.caption(f"السعر العام: **{p['base_price']:.3f}**")

                                price_txt = st.text_input(
                                    "السعر الخاص",
                                    value="",
                                    placeholder="اتركه فارغ = سعر عام",
                                    key=f"add_price_txt__{p['id']}",
                                ).strip()

                                if price_txt != "":
                                    try:
                                        price_val = float(price_txt)
                                    except Exception:
                                        price_val = None

                                    if price_val is None:
                                        st.warning("اكتب رقم صحيح")
                                    else:
                                        add_special_rows.append({
                                            "product_id": p["id"],
                                            "product_name": p["name"],
                                            "price": float(price_val),
                                        })

            with st.form("add_customer_form"):
                name = st.text_input("اسم العميل *")
                phone = st.text_input("الهاتف (اختياري)")
                area = st.text_input("المنطقة (اختياري)")
                opening = st.number_input("دين سابق / رصيد افتتاحي", min_value=0.0, step=1.0, value=0.0)
                submitted = st.form_submit_button("حفظ")

            if submitted:
                if not name.strip():
                    st.error("اسم العميل مطلوب")
                else:
                    customer_id = name.strip().lower().replace(" ", "_")

                    # 1) Save customer
                    doc_set("customers", customer_id, {
                        "name": name.strip(),
                        "phone": phone.strip(),
                        "area": area.strip(),
                        "opening_balance": float(opening),
                        "balance": float(opening),
                        "active": True,
                        "created_at": now_iso(),
                        "updated_at": now_iso(),
                        "created_by": user.get("username", ""),
                    }, merge=True)

                    # 2) Save special prices (optional)
                    if st.session_state.get("add_cust_enable_special") and add_special_rows:
                        uniq = {}
                        for r in add_special_rows:
                            uniq[r["product_id"]] = r

                        for pid, r in uniq.items():
                            doc_id = f"{customer_id}__{pid}"
                            doc_set("customer_prices", doc_id, {
                                "customer_id": customer_id,
                                "customer_name": name.strip(),
                                "product_id": pid,
                                "product_name": r.get("product_name", ""),
                                "price": float(to_float(r.get("price", 0.0))),
                                "active": True,
                                "created_at": now_iso(),
                                "updated_at": now_iso(),
                                "updated_by": user.get("username", ""),
                            }, merge=True)

                    st.success("تمت إضافة العميل ✅" + (" مع أسعار خاصة ✅" if (st.session_state.get("add_cust_enable_special") and add_special_rows) else ""))
                    st.rerun()

        # ===========================
        # ✅ Edit customer info + prices
        # ===========================
        with st.expander("✏️ تعديل معلومات العميل + الأسعار الخاصة", expanded=False):
            customers = col_to_list("customers", where_active=True)
            if not customers:
                st.info("لا يوجد عملاء بعد.")
            else:
                cust_map = {c.get("name", c["id"]): c["id"] for c in customers}
                cust_by_id = {c["id"]: c for c in customers}

                sel_name = st.selectbox(
                    "اختر العميل",
                    options=[""] + list(cust_map.keys()),
                    key="edit_prices_customer_select"
                )

                if sel_name:
                    customer_id = cust_map[sel_name]
                    cust = cust_by_id.get(customer_id, {"id": customer_id})

                    prices_map = _get_customer_prices_map(customer_id)

                    with st.form("edit_customer_info_and_prices_form"):
                        st.markdown("### 🧾 معلومات العميل")
                        new_name = st.text_input("اسم العميل", value=cust.get("name", ""))
                        new_phone = st.text_input("الهاتف", value=cust.get("phone", ""))
                        new_area = st.text_input("المنطقة", value=cust.get("area", ""))

                        st.divider()
                        st.markdown("### 💰 الأسعار الخاصة")
                        st.caption("اترك السعر فارغ = إلغاء السعر الخاص (يرجع للسعر العام).")

                        if not PRODUCTS_FOR_PRICES:
                            st.warning("لا يوجد منتجات. أضف منتجات أولاً من صفحة المستودع.")
                        else:
                            for i in range(0, len(PRODUCTS_FOR_PRICES), 2):
                                cols = st.columns(2, gap="large")
                                pair = PRODUCTS_FOR_PRICES[i:i+2]

                                for j, p in enumerate(pair):
                                    with cols[j]:
                                        pid = p["id"]
                                        st.markdown(f"**🧾 {p['name']}**")
                                        st.caption(f"السعر العام: **{p['base_price']:.3f}**")

                                        current = prices_map.get(pid, {})
                                        current_price = current.get("price", None)
                                        default_txt = "" if current_price is None else str(current_price)

                                        st.text_input(
                                            "السعر الخاص",
                                            value=default_txt,
                                            placeholder="اتركه فارغ = سعر عام",
                                            key=f"edit_price_txt__{customer_id}__{pid}"
                                        )

                        save = st.form_submit_button("💾 حفظ التعديلات")

                    if save:
                        # 1) update customer info
                        doc_set("customers", customer_id, {
                            "name": (new_name or "").strip(),
                            "phone": (new_phone or "").strip(),
                            "area": (new_area or "").strip(),
                            "updated_at": now_iso(),
                        }, merge=True)

                        # 2) update prices
                        if PRODUCTS_FOR_PRICES:
                            for p in PRODUCTS_FOR_PRICES:
                                pid = p["id"]
                                pname = p["name"]

                                txt = (st.session_state.get(f"edit_price_txt__{customer_id}__{pid}") or "").strip()
                                doc_id = f"{customer_id}__{pid}"

                                if txt == "":
                                    # deactivate special price
                                    doc_set("customer_prices", doc_id, {
                                        "active": False,
                                        "updated_at": now_iso(),
                                        "updated_by": user.get("username", ""),
                                    }, merge=True)
                                else:
                                    try:
                                        price_val = float(txt)
                                    except Exception:
                                        price_val = None

                                    if price_val is None:
                                        st.warning(f"سعر غير صالح للمنتج: {pname} — تم تجاهله")
                                        continue

                                    doc_set("customer_prices", doc_id, {
                                        "customer_id": customer_id,
                                        "customer_name": (new_name or "").strip() or cust.get("name", ""),
                                        "product_id": pid,
                                        "product_name": pname,
                                        "price": float(price_val),
                                        "active": True,
                                        "created_at": prices_map.get(pid, {}).get("created_at", now_iso()),
                                        "updated_at": now_iso(),
                                        "updated_by": user.get("username", ""),
                                    }, merge=True)

                        st.success("تم تحديث معلومات العميل والأسعار الخاصة ✅")
                        st.rerun()

        # ===========================
        # ✅ Customers list (edit opening balance + disable)
        # ===========================
        q = st.text_input("🔎 بحث عميل", placeholder="اكتب اسم/منطقة/هاتف...")
        customers = col_to_list("customers", where_active=True)
        if q.strip():
            qq = q.strip().lower()
            customers = [
                c for c in customers
                if qq in (c.get("name", "").lower()
                          + " " + c.get("phone", "").lower()
                          + " " + c.get("area", "").lower()
                          + " " + c.get("id", "").lower())
            ]

        st.markdown("### قائمة العملاء والديون")
        if not customers:
            st.info("لا يوجد عملاء حتى الآن.")
        else:
            rows = []
            for c in sorted(customers, key=lambda x: x.get("name", "")):
                rows.append({
                    "id": c["id"],
                    "name": c.get("name", ""),
                    "phone": c.get("phone", ""),
                    "area": c.get("area", ""),
                    "opening_balance": to_float(c.get("opening_balance", 0)),
                    "balance": to_float(c.get("balance", 0)),
                })

            edited = st.data_editor(
                rows,
                use_container_width=True,
                hide_index=True,
                disabled=["id", "balance"],
                column_config={
                    "opening_balance": st.column_config.NumberColumn("دين سابق", step=1.0),
                },
                key="customers_editor"
            )

            colA, colB = st.columns(2)
            with colA:
                if st.button("💾 حفظ التعديلات", use_container_width=True, key="cust_save_btn"):
                    for r in edited:
                        doc_set("customers", r["id"], {
                            "name": (r.get("name") or "").strip(),
                            "phone": (r.get("phone") or "").strip(),
                            "area": (r.get("area") or "").strip(),
                            "opening_balance": float(to_float(r.get("opening_balance"))),
                            "updated_at": now_iso(),
                        }, merge=True)
                    st.success("تم حفظ التعديلات ✅")
                    st.rerun()

            with colB:
                del_id = st.selectbox("🗑️ حذف عميل", options=[""] + [c["id"] for c in customers], key="cust_del_select")
                if st.button("حذف العميل", use_container_width=True, key="cust_disable_btn"):
                    if del_id:
                        doc_soft_delete("customers", del_id)
                        st.success("تم حذف العميل ✅")
                        st.rerun()

    # ---------------------------
    # Tab 2: Customer statement
    # ---------------------------
    with tabs[1]:
        st.subheader("📊 كشف حساب العميل")

        customers = col_to_list("customers", where_active=True)
        if not customers:
            st.info("أضف عملاء أولًا.")
            return

        cust_map = {c.get("name", c["id"]): c["id"] for c in customers}
        cust_by_id = {c["id"]: c for c in customers}

        cust_name = st.selectbox("اختر العميل", options=[""] + list(cust_map.keys()), key="stmt_customer_select")
        if not cust_name:
            st.info("اختر عميل لعرض كشف الحساب.")
            return

        customer_id = cust_map[cust_name]
        customer = cust_by_id.get(customer_id, {"id": customer_id})

        rows, final_balance, sales_credit, sales_cash, cols, rets = _build_statement(customer)

        st.markdown("### 💰 تحصيل (سداد دين بدون شراء)")
        with st.expander("➕ إضافة سند قبض", expanded=False):
            st.caption("هذا الخيار لسداد دين قديم بدون إنشاء فاتورة شراء.")

            # المبلغ الافتراضي: الدين الحالي إن كان موجب، وإلا 0
            default_amt = float(final_balance) if float(final_balance) > 0 else 0.0

            col1, col2 = st.columns([1.2, 2.8])
            with col1:
                amt = st.number_input("المبلغ المستلم", min_value=0.0, step=0.1, value=float(default_amt), key="collect_amt")

            with col2:
                note = st.text_input("ملاحظة (اختياري)", value="سداد دين قديم", key="collect_note")

            allow_overpay = st.checkbox("السماح بزيادة كرصد دائن للعميل إذا دفع أكثر من الدين", value=True, key="collect_allow_overpay")

            save_btn = st.button("💾 حفظ سند القبض", use_container_width=True, key="collect_save_btn")

            if save_btn:
                amt = float(to_float(amt, 0.0))
                if amt <= 0:
                    st.error("أدخل مبلغ صحيح أكبر من صفر.")
                else:
                    # إذا لا تريد رصيد دائن: لا تسمح يتجاوز الدين الحالي
                    if (not allow_overpay) and float(final_balance) > 0 and amt > float(final_balance):
                        st.warning(f"تم تخفيض المبلغ إلى قيمة الدين الحالي: {float(final_balance):.3f}")
                        amt = float(final_balance)

                    add_collection(customer=customer, amount=amt, user=user, note=note, status="posted")
                    st.success("تم تسجيل سند القبض ✅")
                    st.rerun()

        st.divider()

        total_credit = sum(to_float(s.get("net", s.get("total", 0))) for s in sales_credit)
        total_cash_net = sum(to_float(s.get("net", s.get("total", 0))) for s in sales_cash)
        total_cols = sum(to_float(c.get("amount", 0)) for c in cols)
        total_rets = sum(to_float(r.get("total", 0)) for r in rets)

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("الرصيد الحالي", f"{final_balance:.2f}")
        s2.metric("مبيعات ذمم", f"{total_credit:.2f}")
        s3.metric("مبيعات نقدي", f"{total_cash_net:.2f}")
        s4.metric("تحصيلات", f"{total_cols:.2f}")

               
        st.divider()

        p1, p2 = st.columns([1.2, 2.8])
        with p1:
            paper = st.selectbox("ورق الطباعة", ["80mm", "a4"], index=0, key="stmt_paper")
        with p2:
            if st.button("🖨️ طباعة كشف الحساب", use_container_width=True, key="stmt_print_btn"):
                html = build_customer_full_statement_html(
                    customer=customer,
                    rows=rows,
                    final_balance=final_balance,
                    company_name="نظام المخبز",
                    paper=paper
                )
                show_print_html(html, height=820)

        st.divider()
        st.markdown("### جدول الحركات (كشف الحساب)")

        if not rows:
            st.info("لا توجد حركات.")
            return
                # ✅ أدوات فرز/فلترة للجدول (بدون تغيير البيانات الأصلية)
        st.markdown("### 🔍 فرز / فلترة الحركات")

        # خيارات فلترة مناسبة (تقدر تزيد لاحقاً)
        filter_options = ["الكل", "ذمم", "نقدي", "تحصيل", "مرتجع", "افتتاحي"]

        c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.2, 2.4])

        with c1:
            kind = st.selectbox("عرض", filter_options, index=0, key="stmt_filter_kind")

        with c2:
            sort_dir = st.selectbox("ترتيب", ["الأحدث أولاً", "الأقدم أولاً"], index=0, key="stmt_sort_dir")

        with c3:
            max_rows = st.selectbox("عدد السطور", [20, 50, 100, 200, "الكل"], index=1, key="stmt_max_rows")

        with c4:
            q = st.text_input("بحث (مرجع/نوع/تاريخ)", value="", placeholder="مثال: SALE: أو INV أو تاريخ...", key="stmt_q")

        # نسخة للعمل
        filtered = list(rows or [])

        # فلترة حسب النوع (تعتمد على النص في عمود 'النوع')
        def _match_kind(row_type: str, target: str) -> bool:
            t = (row_type or "")
            if target == "الكل":
                return True
            if target == "ذمم":
                return ("ذمم" in t) or ("آجل" in t)
            if target == "نقدي":
                return ("نقدي" in t) or ("دفع" in t and "سند قبض" not in t)
            if target == "تحصيل":
                return ("تحصيل" in t) or ("سند قبض" in t) or ("قبض" in t)
            if target == "مرتجع":
                return ("مرتجع" in t) or ("خصم دين" in t)
            if target == "افتتاحي":
                return ("افتتاحي" in t) or ("دين سابق" in t)
            return True

        filtered = [r for r in filtered if _match_kind(r.get("النوع", ""), kind)]

        # بحث عام
        qq = (q or "").strip().lower()
        if qq:
            def _row_text(r):
                return f"{r.get('التاريخ','')} {r.get('النوع','')} {r.get('المرجع','')}".lower()
            filtered = [r for r in filtered if qq in _row_text(r)]

        # ترتيب حسب التاريخ (نصياً عندك ISO/مقارب)
        reverse = (sort_dir == "الأحدث أولاً")
        filtered.sort(key=lambda r: (r.get("التاريخ") or ""), reverse=reverse)

        # حد عدد السطور
        if max_rows != "الكل":
            filtered = filtered[:int(max_rows)]

        st.caption(f"النتائج المعروضة: {len(filtered)} / إجمالي الحركات: {len(rows)}")

        # عرض الجدول بعد الفلترة
        st.dataframe(filtered, use_container_width=True, hide_index=True)