import streamlit as st
from datetime import datetime, timezone, timedelta
from io import BytesIO

import pandas as pd

from firebase_config import db
from login import login

from inventory_page import inventory_page
from orders_prep_page import orders_prep_page
from customers_page import customers_page
from distributors_page import distributors_page
 
st.set_page_config(
    page_title="نظام المخبز",
    layout="wide",
    initial_sidebar_state="collapsed"
)

hide_all_streamlit = """
<style>
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}
.stDeployButton {display:none;}
[data-testid="stDecoration"] {display:none;}
</style>
"""
st.markdown(hide_all_streamlit, unsafe_allow_html=True)

# =========================================================
# Helpers (FAST)
# =========================================================
def to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def _today_str_jordan():
    tz = timezone(timedelta(hours=3))
    return datetime.now(tz).strftime("%Y-%m-%d")


def _money(x):
    try:
        return round(float(x), 3)
    except Exception:
        return 0.0


# =========================================================
# Inventory Alerts (cached)
# =========================================================
def _fetch_alerts_raw(limit_each: int = 200):
    """
    🔴 منتهي: qty_on_hand <= 0
    🟡 تحت الحد: qty_on_hand <= min_qty (و min_qty > 0)
    """
    out_count = 0
    low_count = 0
    items = []

    mats = db.collection("materials").where("active", "==", True).limit(limit_each).stream()
    for d in mats:
        x = d.to_dict() or {}
        name = x.get("name") or d.id
        qty = to_float(x.get("qty_on_hand", 0))
        min_qty = to_float(x.get("min_qty", 0))

        if qty <= 0:
            out_count += 1
            items.append({"النوع": "مادة خام", "الاسم": name, "المتوفر": qty, "الحد الأدنى": min_qty, "الحالة": "منتهي"})
        elif min_qty > 0 and qty <= min_qty:
            low_count += 1
            items.append({"النوع": "مادة خام", "الاسم": name, "المتوفر": qty, "الحد الأدنى": min_qty, "الحالة": "قارب على النفاد"})

    prods = db.collection("products").where("active", "==", True).limit(limit_each).stream()
    for d in prods:
        x = d.to_dict() or {}
        name = x.get("name") or d.id
        qty = to_float(x.get("qty_on_hand", 0))
        min_qty = to_float(x.get("min_qty", 0))  # إذا غير موجود = 0

        if qty <= 0:
            out_count += 1
            items.append({"النوع": "منتج", "الاسم": name, "المتوفر": qty, "الحد الأدنى": min_qty, "الحالة": "منتهي"})
        elif min_qty > 0 and qty <= min_qty:
            low_count += 1
            items.append({"النوع": "منتج", "الاسم": name, "المتوفر": qty, "الحد الأدنى": min_qty, "الحالة": "قارب على النفاد"})

    def _rank(r):
        return (0 if r.get("الحالة") == "منتهي" else 1, r.get("المتوفر", 0))
    items.sort(key=_rank)

    return {"out": int(out_count), "low": int(low_count), "total": int(out_count + low_count), "rows": items}


def get_alerts_cached(ttl_seconds: int = 30, limit_each: int = 200):
    now_ts = datetime.now().timestamp()

    cache_ts = st.session_state.get("alerts_ts", 0.0)
    cache_data = st.session_state.get("alerts_cache", None)

    if cache_data is not None and (now_ts - cache_ts) <= ttl_seconds:
        return cache_data

    data = _fetch_alerts_raw(limit_each=limit_each)
    st.session_state["alerts_cache"] = data
    st.session_state["alerts_ts"] = now_ts
    return data


# =========================================================
# Today Sales/Collections Report (cached)
# =========================================================
def _get_today_docs(collection_name: str, today: str, limit=700):
    """
    خفيف: نسحب limit ثم نفلتر محلياً حسب created_at يبدأ بـ YYYY-MM-DD
    (لا نستخدم where على created_at حتى لا نحتاج index)
    """
    docs = db.collection(collection_name).where("active", "==", True).limit(limit).stream()
    out = []
    for d in docs:
        x = d.to_dict() or {}
        created = (x.get("created_at") or "")
        if created.startswith(today):
            out.append({"id": d.id, **x})
    return out


def get_today_report_cached(ttl_seconds=30, limit_sales=900, limit_cols=900):
    now_ts = datetime.now().timestamp()
    ts = st.session_state.get("today_rep_ts", 0.0)
    cached = st.session_state.get("today_rep_cache", None)

    if cached is not None and (now_ts - ts) <= ttl_seconds:
        return cached

    today = _today_str_jordan()

    sales = _get_today_docs("sales", today=today, limit=limit_sales)
    cols = _get_today_docs("collections", today=today, limit=limit_cols)

    # ---- ملخص ----
    total_sales = 0.0
    invoices_count = 0

    cash_sales_total = 0.0
    credit_sales_total = 0.0

    paid_in_sales = 0.0
    collections_total = 0.0

    unpaid_today = 0.0

    rows = []

    # sales
    for s in sales:
        if s.get("status") not in ["posted", "done"]:
            continue

        invoices_count += 1
        net = _money(s.get("net", s.get("total", 0)))
        total_sales += net

        ptype = (s.get("payment_type") or "").lower()
        if ptype == "credit":
            credit_sales_total += net
        else:
            cash_sales_total += net

        paid = _money(s.get("amount_paid", 0))
        paid_in_sales += paid

        unpaid = _money(s.get("unpaid_debt", 0))
        unpaid_today += unpaid

        t = (s.get("created_at", "") or "")[:19].replace("T", " ")
        cust = s.get("customer_name", s.get("customer_id", ""))

        rows.append({
            "الوقت": t,
            "النوع": "بيع",
            "الدفع": "ذمم" if ptype == "credit" else "نقدي",
            "العميل": cust,
            "المبلغ": net,
            "مرجع": f"SALE:{s.get('id','')}",
        })

    # collections
    for c in cols:
        if c.get("status") not in ["posted", "done"]:
            continue

        amt = _money(c.get("amount", 0))
        collections_total += amt

        t = (c.get("created_at", "") or "")[:19].replace("T", " ")
        cust = c.get("customer_name", c.get("customer_id", ""))

        rows.append({
            "الوقت": t,
            "النوع": "قبض",
            "الدفع": "نقدي",
            "العميل": cust,
            "المبلغ": amt,
            "مرجع": f"COL:{c.get('id','')}",
        })

    rows.sort(key=lambda r: r.get("الوقت", ""))

    total_received_today = _money(paid_in_sales + collections_total)
    cash_in_box_today = total_received_today

    data = {
        "today": today,
        "total_sales": _money(total_sales),
        "invoices_count": int(invoices_count),
        "cash_sales_total": _money(cash_sales_total),
        "credit_sales_total": _money(credit_sales_total),
        "paid_in_sales": _money(paid_in_sales),
        "collections_total": _money(collections_total),
        "total_received": total_received_today,
        "cash_in_box": cash_in_box_today,
        "unpaid_today": _money(unpaid_today),
        "rows": rows[-300:],
    }

    st.session_state["today_rep_cache"] = data
    st.session_state["today_rep_ts"] = now_ts
    return data


# =========================================================
# Printing + Export
# =========================================================
def build_today_report_html(rep: dict, company_name="نظام المخبز", paper="a4", cashier_name=""):
    # ✅ دائمًا A4
    width_css = "850px"
    font_css = "14px"

    rows = rep.get("rows", []) or []
    rows = rows[-120:]

    rows_html = ""
    for r in rows:
        rows_html += f"""
        <tr>
          <td>{r.get('الوقت','')}</td>
          <td>{r.get('النوع','')}</td>
          <td>{r.get('الدفع','')}</td>
          <td>{r.get('العميل','')}</td>
          <td>{float(r.get('المبلغ',0)):.3f}</td>
        </tr>
        """

    dt = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<style>
body {{ font-family: Arial; margin:0; padding:0; }}
.wrap {{ width:{width_css}; margin:auto; padding:12px; font-size:{font_css}; }}
.center {{ text-align:center; }}
hr {{ border:none; border-top:1px dashed #999; margin:10px 0; }}
table {{ width:100%; border-collapse: collapse; }}
td,th {{ border-bottom:1px solid #eee; padding:5px 3px; text-align:right; vertical-align:top; }}
.small {{ color:#666; font-size: 0.9em; }}
.sig {{ margin-top: 14px; display:flex; justify-content:space-between; gap: 18px; }}
.sig div {{ width: 50%; border-top:1px solid #ddd; padding-top:8px; }}
.btnbar {{ margin-top:12px; }}
button {{ width:100%; padding:10px; cursor:pointer; }}
@media print {{
  .btnbar {{ display:none; }}
  .wrap {{ width: 100%; }}
}}
</style>
</head>
<body>
<div class="wrap">

<div class="center">
  <div style="font-size:18px;font-weight:800;">{company_name}</div>
  <div style="font-weight:800;margin-top:6px;">كشف مبيعات اليوم</div>
  <div class="small">{rep.get("today","")}</div>
</div>

<hr/>

<div>💰 <b>إجمالي المبيعات:</b> {rep.get("total_sales",0):.3f}</div>
<div>🧾 <b>مبيعات نقدي:</b> {rep.get("cash_sales_total",0):.3f}</div>
<div>📌 <b>مبيعات ذمم:</b> {rep.get("credit_sales_total",0):.3f}</div>

<hr/>

<div>💵 <b>المقبوض اليوم:</b> {rep.get("total_received",0):.3f}</div>
<div>🏦 <b>النقد بالصندوق اليوم:</b> {rep.get("cash_in_box",0):.3f}</div>
<div>🧾 <b>ذمم اليوم (متبقي):</b> {rep.get("unpaid_today",0):.3f}</div>
<div>📦 <b>عدد الفواتير:</b> {rep.get("invoices_count",0)}</div>

<hr/>

<table>
<thead>
<tr>
<th>وقت</th>
<th>نوع</th>
<th>دفع</th>
<th>عميل</th>
<th>مبلغ</th>
</tr>
</thead>
<tbody>
{rows_html if rows_html else "<tr><td colspan='5' class='small'>لا توجد حركات اليوم.</td></tr>"}
</tbody>
</table>

<div class="small" style="margin-top:10px;">
تمت الطباعة: {dt} {(" | المستخدم: " + cashier_name) if cashier_name else ""}
</div>

<div class="sig">
  <div>توقيع المحاسب</div>
  <div>توقيع المستلم</div>
</div>

<div class="btnbar">
  <button onclick="window.print()">🖨️ طباعة</button>
</div>

</div>
</body>
</html>
"""
    return html


def export_today_excel(rep: dict) -> bytes:
    df = pd.DataFrame(rep.get("rows", []) or [])
    cols = ["الوقت", "النوع", "الدفع", "العميل", "المبلغ", "مرجع"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols]

    summary = pd.DataFrame([{
        "التاريخ": rep.get("today", ""),
        "إجمالي المبيعات": rep.get("total_sales", 0),
        "مبيعات نقدي": rep.get("cash_sales_total", 0),
        "مبيعات ذمم": rep.get("credit_sales_total", 0),
        "المقبوض اليوم": rep.get("total_received", 0),
        "النقد بالصندوق اليوم": rep.get("cash_in_box", 0),
        "ذمم اليوم": rep.get("unpaid_today", 0),
        "عدد الفواتير": rep.get("invoices_count", 0),
    }])

    bio = BytesIO()
    # ✅ سيعمل فقط إذا openpyxl موجودة
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        summary.to_excel(writer, index=False, sheet_name="Summary")
        df.to_excel(writer, index=False, sheet_name="Today")
    return bio.getvalue()


# =========================================================
# Session
# =========================================================
if "user" not in st.session_state:
    st.session_state.user = None

if "page" not in st.session_state:
    st.session_state.page = "dashboard"

if "show_alerts" not in st.session_state:
    st.session_state.show_alerts = False


def go(p: str):
    if st.session_state.page != p:
        st.session_state.page = p
        st.rerun()


# =========================================================
# Login
# =========================================================
if st.session_state.user is None:
    login()
    st.stop()

user = st.session_state.user

if user.get("role") != "admin":
    st.error("ليس لديك صلاحية الوصول")
    st.stop()


# =========================================================
# Dashboard
# =========================================================
if st.session_state.page == "dashboard":
    st.markdown("<h2 style='text-align:center;'>لوحة التحكم</h2>", unsafe_allow_html=True)
    st.caption(f"مرحبًا: {user.get('username','')}")

    # ---------- Alerts ----------
    alerts = get_alerts_cached(ttl_seconds=30, limit_each=200)

    st.markdown("""
    <style>
    div[data-testid="column"] button {
        height: 44px !important;
        border-radius: 14px !important;
        border: 1px solid rgba(0,0,0,0.08) !important;
        font-weight: 800 !important;
    }
    .bell-red button { background:#ff4b4b!important; color:white!important; border:0!important; }
    .bell-yellow button { background:#f4c542!important; color:#111!important; border:0!important; }
    .bell-gray button { background:#f1f1f1!important; color:#444!important; border:0!important; }
    </style>
    """, unsafe_allow_html=True)

    if alerts["out"] > 0:
        bell_class = "bell-red"
    elif alerts["low"] > 0:
        bell_class = "bell-yellow"
    else:
        bell_class = "bell-gray"

    bell_label = f"🔔 {alerts['total']}" if alerts["total"] > 0 else "🔔"

    spacer, bell_col = st.columns([9, 1])
    with bell_col:
        st.markdown(f'<div class="{bell_class}">', unsafe_allow_html=True)
        if st.button(bell_label, key="btn_bell", use_container_width=True):
            st.session_state.show_alerts = not st.session_state.show_alerts
        st.markdown("</div>", unsafe_allow_html=True)

    if st.session_state.show_alerts:
        if alerts["total"] == 0:
            st.success("✅ لا يوجد تنبيهات مخزون حالياً")
        else:
            if alerts["out"] > 0:
                st.error(f"🔴 يوجد {alerts['out']} صنف/أصناف منتهية")
            if alerts["low"] > 0:
                st.warning(f"🟡 يوجد {alerts['low']} صنف/أصناف تحت حد إعادة الطلب")
            st.dataframe(alerts["rows"], use_container_width=True, hide_index=True)

    st.divider()

    # ---------- Today Report ----------
    rep = get_today_report_cached(ttl_seconds=30, limit_sales=900, limit_cols=900)

    st.subheader(f"📊 ملخص اليوم ({rep['today']})")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("💰 إجمالي المبيعات", f"{rep['total_sales']:.3f}")
    m2.metric("💵 المقبوض اليوم", f"{rep['total_received']:.3f}")
    m3.metric("🧾 ذمم اليوم (متبقي)", f"{rep['unpaid_today']:.3f}")
    m4.metric("📦 عدد الفواتير", f"{rep['invoices_count']}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🧾 مبيعات نقدي", f"{rep['cash_sales_total']:.3f}")
    c2.metric("📌 مبيعات ذمم", f"{rep['credit_sales_total']:.3f}")
    c3.metric("🏦 النقد بالصندوق اليوم", f"{rep['cash_in_box']:.3f}")
    c4.metric("🧾 سندات القبض اليوم", f"{rep['collections_total']:.3f}")

    # ✅ أدوات: طباعة A4 + تنزيل (بدون خيار ورق)
    a2, a3 = st.columns([1.6, 2.2])

    with a2:
        if st.button("🖨️ طباعة كشف اليوم (A4)", use_container_width=True, key="today_print_btn"):
            html = build_today_report_html(
                rep,
                company_name="نظام المخبز",
                paper="a4",  # ✅ ثابت دائمًا
                cashier_name=user.get("username", "")
            )
            st.components.v1.html(html, height=900, scrolling=True)

    with a3:
        # ✅ Excel إذا openpyxl موجودة، وإلا CSV سريع
        try:
            xbytes = export_today_excel(rep)
            st.download_button(
                "📥 تنزيل Excel (اليوم)",
                data=xbytes,
                file_name=f"today_report_{rep['today']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        except Exception:
            df = pd.DataFrame(rep.get("rows", []) or [])
            csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "📥 تنزيل CSV (اليوم)",
                data=csv_bytes,
                file_name=f"today_report_{rep['today']}.csv",
                mime="text/csv",
                use_container_width=True
            )

    with st.expander("📄 تفاصيل اليوم (بيع + قبض)", expanded=False):
        if not rep["rows"]:
            st.info("لا توجد حركات اليوم حتى الآن.")
        else:
            st.dataframe(rep["rows"], use_container_width=True, hide_index=True)

    st.divider()

    # ---------- Navigation Buttons ----------
    left, center, right = st.columns([1.2, 2.2, 1.2])
    with center:
        if st.button("👥 العملاء", use_container_width=True):
            go("customers")

        if st.button("📦 إدارة المستودع", use_container_width=True):
            go("inventory")

        if st.button("🧑‍🍳 تحضير الأوردرات", use_container_width=True):
            go("orders_prep")

        if st.button("🚚 الموزعين", use_container_width=True):
            go("distributors")

        if st.button("🚪 تسجيل الخروج", use_container_width=True):
            st.session_state.clear()
            st.rerun()


# =========================================================
# Pages
# =========================================================
elif st.session_state.page == "customers":
    customers_page(go, user)

elif st.session_state.page == "inventory":
    inventory_page(go, user)

elif st.session_state.page == "orders_prep":
    orders_prep_page(go, user)

elif st.session_state.page == "distributors":
    distributors_page(go, user)

else:
    st.session_state.page = "dashboard"
    st.rerun()
