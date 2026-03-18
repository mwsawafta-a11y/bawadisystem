# orders_archive_page.py
import streamlit as st
from datetime import datetime, timezone, timedelta, date
from firebase_config import db
from firebase_admin import firestore
import pandas as pd

from utils.helpers import now_iso, to_int, to_float as prep_to_float
from services.firestore_queries import doc_get
from components.printing import (
    build_invoice_html,
    build_receipt_html,
    show_print_html,
)
from .orders_prep_page import _supports_dialog

TZ = timezone(timedelta(hours=3))

# =========================
# Helpers
# =========================
def _iso_start_of_day(d: date) -> str:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ).isoformat()

def _iso_start_of_next_day(d: date) -> str:
    return (datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ) + timedelta(days=1)).isoformat()

def _dt_short(x):
    return (x or "")[:19].replace("T", " ")

# =========================
# Locks (🔥 مهم)
# =========================
def _lock(name):
    if st.session_state.get(name):
        st.warning("⏳ العملية قيد التنفيذ...")
        return False
    st.session_state[name] = True
    return True

def _unlock(name):
    st.session_state[name] = False

# =========================
# Cached
# =========================
@st.cache_data(ttl=120)
def get_products_map(limit=1200):
    docs = db.collection("products").where("active", "==", True).limit(limit).stream()
    return {d.id: d.to_dict() or {} for d in docs}

@st.cache_data(ttl=120)
def get_distributors_list(limit=800):
    docs = db.collection("distributors").where("active", "==", True).limit(limit).stream()
    out = []
    for d in docs:
        x = d.to_dict() or {}
        out.append({"username": d.id, "name": (x.get("name") or d.id)})
    return out

@st.cache_data(ttl=60)
def get_customers_cached(limit=800):
    docs = db.collection("customers").where("active", "==", True).limit(limit).stream()
    return [{"id": d.id, **(d.to_dict() or {})} for d in docs]

@st.cache_data(ttl=60)
def calc_archive_stats_cached(start_iso, end_iso, customer_id, invoice_search, seller_filter):
    q = db.collection("sales").where("active", "==", True).where("status", "==", "done")

    if invoice_search:
        q = q.where("invoice_no", "==", invoice_search)
    else:
        q = q.where("delivered_at", ">=", start_iso).where("delivered_at", "<", end_iso)
        if customer_id:
            q = q.where("customer_id", "==", customer_id)
        if seller_filter:
            q = q.where("seller_username", "==", seller_filter)

    q = q.limit(3000)

    cnt = 0
    total = disc = net = paid = unpaid = extra = 0.0
    cash_cnt = credit_cnt = 0

    for d in q.stream():
        cnt += 1
        x = d.to_dict() or {}
        total += prep_to_float(x.get("total", 0))
        disc += prep_to_float(x.get("discount", 0))
        net += prep_to_float(x.get("net", 0))
        paid += prep_to_float(x.get("amount_paid", 0))
        unpaid += prep_to_float(x.get("unpaid_debt", 0))
        extra += prep_to_float(x.get("extra_credit", 0))

        if x.get("payment_type") == "cash":
            cash_cnt += 1
        elif x.get("payment_type") == "credit":
            credit_cnt += 1

    return {
        "cnt": cnt,
        "total": total,
        "disc": disc,
        "net": net,
        "paid": paid,
        "unpaid": unpaid,
        "extra": extra,
        "cash_cnt": cash_cnt,
        "credit_cnt": credit_cnt,
    }

# =========================
# Main Page
# =========================
def orders_archive_page(go, user):

    st.markdown("## 📁 أرشيف الفواتير")

    if user.get("role") not in ("admin", "owner", "superadmin"):
        st.error("غير مصرح")
        return

    if st.button("⬅️ رجوع"):
        go("orders_prep")

    today = datetime.now(TZ).date()

    col1, col2, col3 = st.columns(3)

    with col1:
        d_from = st.date_input("من", value=today - timedelta(days=6))

    with col2:
        d_to = st.date_input("إلى", value=today)

    with col3:
        invoice_search = st.text_input("رقم الفاتورة")

    customers = get_customers_cached()
    cust_map = {c.get("name", c["id"]): c["id"] for c in customers}

    cust_name = st.selectbox("العميل", ["(الكل)"] + list(cust_map.keys()))
    customer_id = "" if cust_name == "(الكل)" else cust_map[cust_name]

    start_iso = _iso_start_of_day(d_from)
    end_iso = _iso_start_of_next_day(d_to)

    st.divider()

    # =====================
    # Stats
    # =====================
    if st.button("⚡ احسب الإحصائيات"):
        st.session_state["stats"] = calc_archive_stats_cached(
            start_iso, end_iso, customer_id, invoice_search, ""
        )

    stats = st.session_state.get("stats")

    if stats:
        st.write(stats)

    st.divider()

    # =====================
    # Results
    # =====================
    q = db.collection("sales").where("status", "==", "done").where("active", "==", True)

    if invoice_search:
        q = q.where("invoice_no", "==", invoice_search)
    else:
        q = q.where("delivered_at", ">=", start_iso).where("delivered_at", "<", end_iso)

    docs = list(q.limit(100).stream())

    rows = []
    for d in docs:
        x = d.to_dict() or {}
        x["id"] = d.id
        rows.append(x)

    if not rows:
        st.info("لا يوجد بيانات")
        return

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True)

    # =====================
    # Print
    # =====================
    for r in rows[:10]:
        sid = r["id"]

        c1, c2 = st.columns(2)

        with c1:
            if st.button(f"🖨️ {sid}"):
                st.session_state["print_sid"] = sid
                st.session_state["print_mode"] = "invoice"

        with c2:
            if st.button(f"🧾 {sid}"):
                st.session_state["print_sid"] = sid
                st.session_state["print_mode"] = "receipt"

    if st.session_state.get("print_sid"):
        sale = doc_get("sales", st.session_state["print_sid"]) or {}
        cust = doc_get("customers", sale.get("customer_id", "")) or {}

        if st.session_state.get("print_mode") == "receipt":
            html = build_receipt_html(sale, customer=cust)
        else:
            html = build_invoice_html(sale, customer=cust)

        show_print_html(html)