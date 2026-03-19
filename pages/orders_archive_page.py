# orders_archive_page.py
import streamlit as st
from datetime import datetime, timezone, timedelta, date
from io import BytesIO

from firebase_config import db
from firebase_admin import firestore
import pandas as pd

from utils.helpers import to_float as prep_to_float
from services.firestore_queries import doc_get
from components.printing import (
    build_invoice_html,
    build_receipt_html,
    show_print_html,
)

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


def export_archive_excel(df: pd.DataFrame, stats: dict | None, d_from, d_to) -> bytes:
    bio = BytesIO()

    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        if stats:
            summary_df = pd.DataFrame([{
                "من": str(d_from),
                "إلى": str(d_to),
                "عدد الفواتير": stats.get("cnt", 0),
                "الإجمالي": stats.get("total", 0),
                "الخصم": stats.get("disc", 0),
                "الصافي": stats.get("net", 0),
                "المدفوع": stats.get("paid", 0),
                "متبقي ذمم": stats.get("unpaid", 0),
                "زيادة كرصد": stats.get("extra", 0),
                "عدد النقدي": stats.get("cash_cnt", 0),
                "عدد الذمم": stats.get("credit_cnt", 0),
            }])
            summary_df.to_excel(writer, index=False, sheet_name="Summary")

        df.to_excel(writer, index=False, sheet_name="Results")

    return bio.getvalue()


# =========================
# Cached
# =========================
@st.cache_data(ttl=120)
def get_distributors_list(limit=800):
    docs = db.collection("distributors").where("active", "==", True).limit(limit).stream()
    out = []
    for d in docs:
        x = d.to_dict() or {}
        out.append({
            "username": d.id,
            "name": (x.get("name") or d.id).strip(),
        })
    out.sort(key=lambda r: (r.get("name") or r.get("username") or ""))
    return out


@st.cache_data(ttl=120)
def get_customers_cached(limit=800):
    docs = db.collection("customers").where("active", "==", True).limit(limit).stream()
    out = []
    for d in docs:
        out.append({"id": d.id, **(d.to_dict() or {})})
    out.sort(key=lambda r: (r.get("name") or r.get("id") or ""))
    return out


@st.cache_data(ttl=60)
def calc_archive_stats_cached(start_iso, end_iso, customer_id, invoice_search, seller_filter):
    HARD_CAP = 1000

    q = (
        db.collection("sales")
        .where("active", "==", True)
        .where("status", "==", "done")
    )

    if invoice_search:
        q = q.where("invoice_no", "==", invoice_search).limit(HARD_CAP)
    else:
        q = q.where("delivered_at", ">=", start_iso).where("delivered_at", "<", end_iso)

        if customer_id:
            q = q.where("customer_id", "==", customer_id)

        if seller_filter:
            q = q.where("seller_username", "==", seller_filter)

        q = q.order_by("delivered_at", direction=firestore.Query.DESCENDING).limit(HARD_CAP)

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
    st.markdown("<h2 style='text-align:center;'>📁 أرشيف الفواتير + إحصائيات</h2>", unsafe_allow_html=True)
    st.caption("يعرض الفواتير المُسلّمة فقط. التاريخ افتراضيًا = اليوم فقط لتخفيف الضغط.")
    st.divider()

    if user.get("role") not in ("admin", "owner", "superadmin"):
        st.error("غير مصرح")
        return

    if st.button("⬅️ رجوع", key="arch_back"):
        go("app")

    today = datetime.now(TZ).date()

    st.session_state.setdefault("arch_from", today)
    st.session_state.setdefault("arch_to", today)
    st.session_state.setdefault("arch_load_customers", False)
    st.session_state.setdefault("arch_stats", None)
    st.session_state.setdefault("arch_stats_sig", None)
    st.session_state.setdefault("arch_show_print_tools", False)
    st.session_state.setdefault("arch_print_sid", None)
    st.session_state.setdefault("arch_print_mode", "invoice")

    st.markdown("### 🔎 الفلاتر")

    dist_accounts = get_distributors_list()
    dist_display = [f"{a['username']} — {a.get('name', a['username'])}" for a in dist_accounts]
    dist_map = {f"{a['username']} — {a.get('name', a['username'])}": a["username"] for a in dist_accounts}

    c1, c2, c3, c4 = st.columns([1.1, 1.1, 2.0, 2.0])

    with c1:
        d_from = st.date_input("من", key="arch_from")

    with c2:
        d_to = st.date_input("إلى", key="arch_to")

    with c3:
        invoice_search = st.text_input("بحث برقم الفاتورة", key="arch_invoice_search").strip()

    with c4:
        pick_seller = st.selectbox(
            "الموزّع (اختياري)",
            options=["(الكل)"] + dist_display,
            key="arch_seller_pick"
        )

    seller_filter = "" if pick_seller == "(الكل)" else dist_map.get(pick_seller, "")

    if d_to < d_from:
        st.error("تاريخ (إلى) يجب أن يكون أكبر من أو يساوي تاريخ (من)")
        return

    cc1, cc2 = st.columns([1.2, 2.8])

    with cc1:
        if st.button("👥 تحميل العملاء", key="arch_load_customers_btn", use_container_width=True):
            st.session_state["arch_load_customers"] = True
            st.rerun()

    with cc2:
        if st.session_state.get("arch_load_customers", False):
            cust_list = get_customers_cached(limit=800)
            cust_map = {c.get("name", c["id"]): c["id"] for c in cust_list}
            cust_name = st.selectbox(
                "العميل (اختياري)",
                options=["(الكل)"] + list(cust_map.keys()),
                index=0,
                key="arch_customer_pick"
            )
            customer_id = "" if cust_name == "(الكل)" else cust_map.get(cust_name, "")
        else:
            st.info("العملاء غير محمّلين لتخفيف الضغط. اضغط (تحميل العملاء) إذا أردت الفرز حسب عميل.")
            customer_id = ""

    def _set_today():
        st.session_state["arch_from"] = today
        st.session_state["arch_to"] = today

    def _clear_search():
        st.session_state["arch_invoice_search"] = ""

    b1, b2 = st.columns(2)
    with b1:
        st.button("📌 اليوم", use_container_width=True, on_click=_set_today)
    with b2:
        st.button("🧹 تصفير البحث", use_container_width=True, on_click=_clear_search)

    start_iso = _iso_start_of_day(d_from)
    end_iso = _iso_start_of_next_day(d_to)

    sig = (start_iso, end_iso, customer_id, invoice_search, seller_filter)

    st.divider()
    st.markdown("### 📊 الإحصائيات")

    if st.button("⚡ احسب الإحصائيات", key="arch_calc_stats"):
        st.session_state["arch_stats_sig"] = sig
        st.session_state["arch_stats"] = calc_archive_stats_cached(
            start_iso=start_iso,
            end_iso=end_iso,
            customer_id=customer_id,
            invoice_search=invoice_search,
            seller_filter=seller_filter,
        )
        st.rerun()

    stats = st.session_state["arch_stats"] if st.session_state.get("arch_stats_sig") == sig else None

    a, b, c, d = st.columns(4)
    e, f, g, h = st.columns(4)

    if stats:
        a.metric("عدد الفواتير", f"{stats['cnt']}")
        b.metric("الإجمالي", f"{stats['total']:.2f}")
        c.metric("الخصم", f"{stats['disc']:.2f}")
        d.metric("الصافي", f"{stats['net']:.2f}")
        e.metric("المدفوع", f"{stats['paid']:.2f}")
        f.metric("متبقي ذمم", f"{stats['unpaid']:.2f}")
        g.metric("زيادة كرصد", f"{stats['extra']:.2f}")
        h.metric("نقدي/ذمم", f"{stats['cash_cnt']} / {stats['credit_cnt']}")
    else:
        a.metric("عدد الفواتير", "—")
        b.metric("الإجمالي", "—")
        c.metric("الخصم", "—")
        d.metric("الصافي", "—")
        e.metric("المدفوع", "—")
        f.metric("متبقي ذمم", "—")
        g.metric("زيادة كرصد", "—")
        h.metric("نقدي/ذمم", "—")

    st.divider()
    st.markdown("### 🧾 النتائج")

    PAGE_SIZE = 30

    q = (
        db.collection("sales")
        .where("status", "==", "done")
        .where("active", "==", True)
    )

    if invoice_search:
        q = q.where("invoice_no", "==", invoice_search).limit(PAGE_SIZE)
    else:
        q = q.where("delivered_at", ">=", start_iso).where("delivered_at", "<", end_iso)

        if customer_id:
            q = q.where("customer_id", "==", customer_id)

        if seller_filter:
            q = q.where("seller_username", "==", seller_filter)

        q = q.order_by("delivered_at", direction=firestore.Query.DESCENDING).limit(PAGE_SIZE)

    docs = list(q.stream())

    rows = []
    for d in docs:
        x = d.to_dict() or {}
        x["id"] = d.id
        rows.append(x)

    if not rows:
        st.info("لا توجد بيانات")
        return

    df = pd.DataFrame([{
        "رقم": r.get("invoice_no") or r.get("ref") or r["id"],
        "التاريخ": _dt_short(r.get("delivered_at") or r.get("updated_at") or r.get("created_at")),
        "العميل": r.get("customer_name") or "—",
        "الموزّع": r.get("seller_username") or "—",
        "الدفع": "ذمم" if r.get("payment_type") == "credit" else ("نقدي" if r.get("payment_type") == "cash" else "—"),
        "الصافي": float(prep_to_float(r.get("net", 0))),
    } for r in rows])

    st.dataframe(df, use_container_width=True, hide_index=True)

    download_col1, download_col2 = st.columns(2)

    with download_col1:
        csv_data = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="⬇️ تحميل CSV",
            data=csv_data,
            file_name=f"archive_{d_from}_{d_to}.csv",
            mime="text/csv",
            key="arch_download_csv"
        )

    with download_col2:
        try:
            xlsx_data = export_archive_excel(df, stats, d_from, d_to)
            st.download_button(
                label="⬇️ تحميل Excel",
                data=xlsx_data,
                file_name=f"archive_{d_from}_{d_to}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="arch_download_xlsx"
            )
        except Exception as e:
            st.warning(f"تعذر إنشاء ملف Excel: {e}")

    cprint1, cprint2 = st.columns([1, 3])
    with cprint1:
        if st.button("🖨️ إظهار الطباعة", key="arch_toggle_print_tools", use_container_width=True):
            st.session_state["arch_show_print_tools"] = not st.session_state.get("arch_show_print_tools", False)
            st.rerun()

    with cprint2:
        if st.session_state.get("arch_show_print_tools", False):
            st.info("أدوات الطباعة ظاهرة الآن")

    if st.session_state.get("arch_show_print_tools", False):
        st.markdown("### 🖨️ طباعة (آخر النتائج)")
        for r in rows[:10]:
            sid = r["id"]
            inv = r.get("invoice_no") or r.get("ref") or sid
            ptype = r.get("payment_type")
            paid = float(prep_to_float(r.get("amount_paid", 0)))

            l, b1, b2 = st.columns([4.2, 0.9, 0.9])
            with l:
                st.markdown(f"**{inv}** — {r.get('customer_name','—')} | صافي: **{float(prep_to_float(r.get('net',0))):.2f}**")

            with b1:
                if st.button("🖨️ فاتورة", use_container_width=True, key=f"arch_inv_{sid}"):
                    st.session_state["arch_print_sid"] = sid
                    st.session_state["arch_print_mode"] = "invoice"
                    st.rerun()

            with b2:
                can_rec = (ptype == "cash") and (paid > 0)
                if st.button("🧾 قبض", use_container_width=True, key=f"arch_rec_{sid}", disabled=not can_rec):
                    st.session_state["arch_print_sid"] = sid
                    st.session_state["arch_print_mode"] = "receipt"
                    st.rerun()

            st.divider()

    if st.session_state.get("arch_show_print_tools", False) and st.session_state.get("arch_print_sid"):
        sid = st.session_state["arch_print_sid"]
        mode = st.session_state.get("arch_print_mode", "invoice")

        sale = doc_get("sales", sid) or {}
        sale["id"] = sid

        cust_id = sale.get("customer_id") or ""
        cust = doc_get("customers", cust_id) or {} if cust_id else {}

        if mode == "receipt":
            html = build_receipt_html(sale, customer=cust, company_name="مخابز البوادي", paper="80mm")
        else:
            html = build_invoice_html(sale, customer=cust, company_name="مخابز البوادي", paper="80mm")

        show_print_html(html, height=900)