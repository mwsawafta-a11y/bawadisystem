# orders_archive_page.py
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime, timezone, timedelta, date
from firebase_config import db
from firebase_admin import firestore
import pandas as pd
from utils.helpers import now_iso, to_int
from services.firestore_queries import doc_get
TZ = timezone(timedelta(hours=3))  # Jordan

from utils.helpers import now_iso, to_int, to_float as prep_to_float
from services.firestore_queries import doc_get
from components.printing import (
    build_invoice_html,
    build_receipt_html,
    show_print_html,
)
from .orders_prep_page import _supports_dialog

def _iso_start_of_day(d: date) -> str:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ).isoformat()

def _iso_start_of_next_day(d: date) -> str:
    return (datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=TZ) + timedelta(days=1)).isoformat()

def _dt_short(x):
    return (x or "")[:19].replace("T", " ")

# ✅ استيراد نفس دوال الطباعة والمساعدات من orders_prep_page حتى يكون نفس شكل الفاتورة 100%
# إذا اسم الملف عندك مختلف عدّله هنا
from .orders_prep_page import (
    to_float as prep_to_float,
    _supports_dialog,
    build_invoice_html,
    build_receipt_html,
    show_print_html,
)




#----------------------------------
# تاب الموزعين - اغلاق يومي 
#_________________________________________


# --------- products cache (names only for display) ----------
def get_products_map(limit=1200):
    if "products_map_daily" not in st.session_state:
        docs = db.collection("products").where("active", "==", True).limit(limit).stream()
        mp = {}
        for d in docs:
            x = d.to_dict() or {}
            mp[d.id] = {"id": d.id, **x}
        st.session_state["products_map_daily"] = mp
    return st.session_state["products_map_daily"]

# --------- distributor accounts (username) ----------
# --------- distributors list (from distributors collection) ----------
def get_distributors_list(limit=800):
    docs = (
        db.collection("distributors")
        .where("active", "==", True)
        .limit(limit)
        .stream()
    )
    out = []
    for d in docs:
        x = d.to_dict() or {}
        # حسب صورتك: doc id هو نفسه username (ali, mazen, seli...)
        out.append({
            "username": d.id,                         # هذا هو seller_username
            "name": (x.get("name") or d.id).strip(),  # اسم العرض
        })
    out.sort(key=lambda r: (r.get("name") or r.get("username") or ""))
    return out
#---------------التاكد من الكميات قبل اضافتها للموزع
def product_stock_qty(prod: dict) -> int:
    # أهم حقل عندك بالمستودع حسب الكود: qty_on_hand
    for k in ("qty_on_hand", "stock", "qty", "quantity", "available_qty", "in_stock"):
        if k in prod:
            try:
                return int(float(prod.get(k) or 0))
            except Exception:
                return 0
    return 0
#------انتهى---------التاكد من الكميات قبل اضافتها للموزع

def get_sellers_from_sales(day: date, limit=2000):
    start = _iso_start_of_day(day)
    end = _iso_start_of_next_day(day)

    q = (
        db.collection("sales")
        .where("active", "==", True)
        .where("created_at", ">=", start)
        .where("created_at", "<", end)
        .limit(limit)
    )

    sellers = set()
    for d in q.stream():
        x = d.to_dict() or {}
        for k in ("seller_username", "delivered_by", "created_by"):
            v = (x.get(k) or "").strip()
            if v:
                sellers.add(v)

    return sorted(list(sellers))
 # --------- sales query by day + seller_username ----------
def get_sales_for_day_and_seller(day: date, seller_username: str, limit=2000):
    start = _iso_start_of_day(day)
    end = _iso_start_of_next_day(day)

    q = (
        db.collection("sales")
        .where("active", "==", True)
        .where("created_at", ">=", start)
        .where("created_at", "<", end)
        .limit(limit)
    )

    out = []
    for d in q.stream():
        x = d.to_dict() or {}
        x["id"] = d.id

        su = (x.get("seller_username") or "").strip()
        dbv = (x.get("delivered_by") or "").strip()
        cb  = (x.get("created_by") or "").strip()

        if seller_username in (su, dbv, cb):
            out.append(x)

    out.sort(key=lambda r: (r.get("created_at") or ""))
    return out

def _line_qty(line: dict) -> int:
    for k in ("qty", "quantity", "count", "pieces", "units", "unit_qty"):
        if k in line:
            return to_int(line.get(k, 0), 0)
    return 0

def _line_product_id(line: dict) -> str:
    v = (line.get("product_id") or line.get("pid") or line.get("id") or "").strip()
    return v

def _line_product_name(line: dict) -> str:
    return (line.get("product_name") or line.get("name") or "").strip()

def summarize_from_sales_lines(sales_docs: list):
    """
    حسب بياناتك:
      lines: array
      payment_type: cash / ذمم ...
      total: رقم
    """
    sold_total = {}
    sold_cash = {}
    sold_credit = {}

    cash_total = 0.0
    credit_total = 0.0
    cash_count = 0
    credit_count = 0

    for s in sales_docs:
        pay = (s.get("payment_type") or "").strip().lower()
        total = prep_to_float(s.get("total", 0.0), 0.0)

        is_cash = pay in ("cash", "نقد", "نقدي")
        is_credit = pay in ("credit", "debt", "ذمم", "آجل", "اجل")

        if is_cash:
            cash_total += float(total); cash_count += 1
        elif is_credit:
            credit_total += float(total); credit_count += 1

        for ln in (s.get("lines", []) or []):
            qty = _line_qty(ln)
            if qty <= 0:
                continue
            pid = _line_product_id(ln)
            pname = _line_product_name(ln)
            key = pid if pid else (pname if pname else "—")

            sold_total[key] = sold_total.get(key, 0) + qty
            if is_cash:
                sold_cash[key] = sold_cash.get(key, 0) + qty
            elif is_credit:
                sold_credit[key] = sold_credit.get(key, 0) + qty

    return sold_total, sold_cash, sold_credit, float(cash_total), float(credit_total), int(cash_count), int(credit_count)

# --------- daily load/close docs (keyed by day + seller_username) ----------
def daily_doc_id(day: date, seller_username: str) -> str:
    return f"{day.strftime('%Y-%m-%d')}_{seller_username}"

def get_daily_load(day: date, seller_username: str):
    doc_id = daily_doc_id(day, seller_username)
    snap = db.collection("dist_daily_loads").document(doc_id).get()
    return (snap.to_dict() if snap.exists else None), doc_id

def set_daily_load(doc_id: str, payload: dict):
    db.collection("dist_daily_loads").document(doc_id).set(payload, merge=True)
def merge_dict_qty(a: dict, b: dict) -> dict:
    out = dict(a or {})
    for k, v in (b or {}).items():
        out[k] = int(out.get(k, 0)) + int(to_int(v, 0))
    return out


@firestore.transactional
def tx_add_daily_load_move(transaction, doc_id: str, day: date, seller_username: str, payload_loads: dict, user: dict):
    ref = db.collection("dist_daily_loads").document(doc_id)
    snap = ref.get(transaction=transaction)

    cur = snap.to_dict() if snap.exists else {}
    cur_loads = cur.get("loads", {}) or {}
    cur_moves = cur.get("moves", []) or []

    new_loads = merge_dict_qty(cur_loads, payload_loads)

    move_id = f"mv_{day.strftime('%Y%m%d')}_{datetime.now(TZ).strftime('%H%M%S%f')[:9]}"

    move = {
        "id": move_id,
        "at": now_iso(),
        "by": (user or {}).get("username", ""),
        "loads": {pid: int(to_int(q, 0)) for pid, q in (payload_loads or {}).items()},

        # ✅ حقول إغلاق الحركة
        "closed": False,
        "closed_at": "",
        "closed_by": "",
        "leftover": {},
        "cash_delivered": 0.0,
    }

    base = {
        "date": day.strftime("%Y-%m-%d"),
        "seller_username": seller_username,
        "loads": new_loads,          # ✅ مجموع التحميلات
        "moves": cur_moves + [move], # ✅ سجل كل تحميلة
        "active": True,
        "updated_at": now_iso(),
    }
    if not snap.exists:
        base["created_at"] = now_iso()
        base["created_by"] = (user or {}).get("username", "")

    transaction.set(ref, base, merge=True)

def get_daily_close(day: date, seller_username: str):
    doc_id = daily_doc_id(day, seller_username)
    snap = db.collection("dist_daily_closings").document(doc_id).get()
    return (snap.to_dict() if snap.exists else None), doc_id

def set_daily_close(doc_id: str, payload: dict):
    db.collection("dist_daily_closings").document(doc_id).set(payload, merge=True)

@firestore.transactional
def tx_close_move(transaction, doc_id: str, move_id: str, cash_delivered: float, user: dict):
    ref = db.collection("dist_daily_loads").document(doc_id)
    snap = ref.get(transaction=transaction)

    if not snap.exists:
        raise ValueError("لا يوجد تحميلات لهذا اليوم")

    data = snap.to_dict() or {}
    moves = data.get("moves", []) or []

    found = False
    for mv in moves:
        if mv.get("id") == move_id:
            found = True
            if mv.get("closed") is True:
                raise ValueError("الحركة مغلقة مسبقاً")

            mv["closed"] = True
            mv["closed_at"] = now_iso()
            mv["closed_by"] = (user or {}).get("username", "")
            mv["cash_delivered"] = float(prep_to_float(cash_delivered, 0.0))
            break

    if not found:
        raise ValueError("لم يتم العثور على الحركة")

    transaction.update(ref, {
        "moves": moves,
        "updated_at": now_iso(),
    })


# --------- the new tab UI ----------
def distributor_daily_tab(go, user):
    st.markdown("### 🚚 محاسبة موزّع يومية (إضافة)")
    st.caption("اعتمادًا على seller_username | تحميل صباح (حبّات) + ملخص فواتير اليوم (حبّات + نقد/ذمم) + إغلاق يوم")
    st.divider()

    c1, c2 = st.columns([1.2, 2.0])
    with c1:
        day = st.date_input("📅 التاريخ", value=date.today(), key="dd_day")

        # ✅ الحل 1: الموزعين من distributors (بدون الاعتماد على فواتير اليوم)
        dist_accounts = get_distributors_list()

    with c2:
        if not dist_accounts:
            st.error("لا يوجد موزعين في قاعدة البيانات (distributors).")
            return

        display = [f"{a['username']} — {a.get('name', a['username'])}" for a in dist_accounts]
        display_to_username = {f"{a['username']} — {a.get('name', a['username'])}": a["username"] for a in dist_accounts}

        pick = st.selectbox(
            "👤 اختر الموزّع",
            options=[""] + display,
            key="dd_seller2"
        )

    seller_username = display_to_username.get(pick, "").strip()
    if not seller_username:
        st.info("اختر موزّع.")
        return
    
    if not seller_username:
        st.info("اختر موزّع.")
        return
    if not seller_username:
        st.error("فشل تحديد username.")
        return

    # --- Sales for day + seller_username (fast query) ---
    sales_docs = get_sales_for_day_and_seller(day, seller_username, limit=1500)
    sold_total, sold_cash, sold_credit, cash_total, credit_total, cash_count, credit_count = summarize_from_sales_lines(sales_docs)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("🧾 فواتير اليوم", f"{len(sales_docs)}")
    m2.metric("💵 نقد", f"{cash_count} | {cash_total:.3f}")
    m3.metric("🧾 ذمم", f"{credit_count} | {credit_total:.3f}")
    m4.metric("🍞 أصناف (حبّات)", f"{len(sold_total)}")

    st.divider()

    # --- Units summary ---
    st.subheader("🍞 ملخص الحبات من فواتير اليوم")
    products_map = get_products_map()
    rows = []
    for key, qty in sorted(sold_total.items(), key=lambda x: x[0]):
        # key قد يكون product_id أو اسم منتج، نحاول نطلع اسم المنتج لو هو id
        name = (products_map.get(key, {}) or {}).get("name") if key in products_map else key
        rows.append({
            "المنتج": name,
            "المفتاح": key,
            "إجمالي": qty,
            "نقد": sold_cash.get(key, 0),
            "ذمم": sold_credit.get(key, 0),
        })
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("لا توجد حبات مسجلة في فواتير هذا اليوم لهذا الموزّع.")

    st.divider()

    # --- Morning load ---
    st.subheader("📦 تحميل للموزّع (يمكن أكثر من مرة باليوم)")
# --- Loads (multi-moves per day) ---
 
    products_map = get_products_map()
    all_products = [{"id": pid, "name": (v.get("name") or pid)} for pid, v in products_map.items()]
    all_products.sort(key=lambda r: (r["name"] or ""))

    load_doc, load_doc_id = get_daily_load(day, seller_username)
    if load_doc:
        moves = load_doc.get("moves", []) or []

        st.subheader("🧾 الحركات لهذا اليوم")

        if not moves:
            st.info("لا يوجد حركات بعد.")
        else:
            for mv in moves:
                mv_time = _dt_short(mv.get("at", ""))
                mv_id = mv.get("id")
                mv_closed = mv.get("closed") is True

                title = f"حركة: {mv_time} | {'✅ مغلقة' if mv_closed else '🟡 مفتوحة'}"
                with st.expander(title, expanded=not mv_closed):

                    loads_mv = mv.get("loads", {}) or {}
                    for pid, q in loads_mv.items():
                        nm = (products_map.get(pid, {}) or {}).get("name", pid)
                        st.write(f"{nm}: {q}")

                    if mv_closed:
                        st.success(f"مغلقة | نقد مسلّم: {mv.get('cash_delivered', 0)}")
                        continue

                    # إدخال الإغلاق
                    st.markdown("### 🔒 إغلاق الحركة")

                    cash_delivered = st.number_input(
                        "💰 النقد المسلّم لهذه الحركة",
                        min_value=0.0,
                        step=0.5,
                        value=0.0,
                        key=f"mv_cash_{mv_id}",
                    )

                    if st.button("🔒 إغلاق الحركة", key=f"mv_close_{mv_id}"):

                        if cash_delivered <= 0:
                            st.warning("أدخل قيمة النقد أولاً")
                        else:
                            tx_close_move(
                                db.transaction(),
                                load_doc_id,
                                mv_id,
                                cash_delivered,
                                user,
                            )

                            st.success("تم إغلاق الحركة ✅")
                            st.rerun()

    close_doc, _ = get_daily_close(day, seller_username)

    if close_doc:
        st.warning("اليوم مُغلق — لا يمكن إضافة تحميلات جديدة.")
        st.info("ملاحظة: التحميلات هنا للتوثيق المحاسبي فقط ولا تخصم من المستودع لأن الفواتير هي التي تخصم مباشرة.")

    # ✅ اعرض المجموع الحالي + سجل التحميلات (إن وجد)
    if load_doc:
        loads_total = load_doc.get("loads", {}) or {}
        view = []
        for pid, q in sorted(loads_total.items(), key=lambda x: x[0]):
            view.append({
                "المنتج": (products_map.get(pid, {}) or {}).get("name", pid),
                "المعرف": pid,
                "إجمالي التحميل": to_int(q, 0),
            })
        st.success("✅ إجمالي التحميلات لهذا اليوم")
        st.dataframe(view, use_container_width=True, hide_index=True)

        moves = load_doc.get("moves", []) or []
        with st.expander("📜 سجل التحميلات (كل مرة تحميل)"):
            if not moves:
                st.info("لا يوجد سجل تحميلات بعد.")
            else:
                rows_moves = []
                for mv in reversed(moves[-50:]):  # آخر 50 حركة
                    loads_mv = mv.get("loads", {}) or {}
                    # اختصار عرض السلة
                    summary = " | ".join([
                        f"{(products_map.get(pid, {}) or {}).get('name', pid)}: {to_int(q,0)}"
                        for pid, q in loads_mv.items()
                    ])
                    rows_moves.append({
                        "وقت": _dt_short(mv.get("at", "")),
                        "بواسطة": mv.get("by", ""),
                        "تفاصيل": summary,
                    })
                st.dataframe(rows_moves, use_container_width=True, hide_index=True)

    st.divider()

    # ✅ إدخال تحميل جديد دائماً (حتى لو load_doc موجود) بشرط اليوم غير مغلق
    default_names = []
    for k in (load_doc.get("loads", {}).keys() if load_doc else []):
        if k in products_map:
            default_names.append(products_map[k].get("name", k))

    chosen = st.multiselect(
        "اختر الأصناف التي حمّلتها للموزّع",
        options=[p["name"] for p in all_products],
        default=default_names[:20],
        key="dd_load_choose"
    )

    name_to_id = {p["name"]: p["id"] for p in all_products}
    loads_in = {}
    for nm in chosen:
        pid = name_to_id[nm]
        loads_in[pid] = st.number_input(nm, min_value=0, step=1, value=0, key=f"dd_load_{pid}")

    if st.button("➕ إضافة تحميل", use_container_width=True, key="dd_add_load", disabled=bool(close_doc)):

        payload_loads = {pid: int(q) for pid, q in loads_in.items() if to_int(q, 0) > 0}
        if not payload_loads:
            st.error("أدخل على الأقل صنف واحد بكمية > 0.")
            st.stop()



        try:
            tx_add_daily_load_move(db.transaction(), load_doc_id, day, seller_username, payload_loads, user)
            st.success("تمت إضافة التحميل ✅")
            st.rerun()
        except Exception as e:
            st.error(f"فشل إضافة التحميل: {e}")
    st.divider()

    # --- Closing ---
    st.subheader("🔒 إغلاق اليوم (المتبقي + النقد المسلّم)")
    close_doc, close_doc_id = get_daily_close(day, seller_username)

    load_doc, _ = get_daily_load(day, seller_username)
    if not load_doc:
        st.info("سجّل تحميل الصباح أولاً.")
        return

    loads = load_doc.get("loads", {}) or {}

    # المتوقع المتبقي = التحميل - المبيعات (من الفواتير) (نستخدم نفس key = product_id)
    expected_left = {}
    for pid, lq in loads.items():
        expected_left[pid] = to_int(lq,0) - to_int(sold_total.get(pid,0),0)

    if close_doc:
        st.success("هذا اليوم مُغلق ✅ (عرض فقط)")
        leftover = close_doc.get("leftover", {}) or {}
        cash_delivered = prep_to_float(close_doc.get("cash_delivered", 0.0), 0.0)

        report = []
        for pid, exp in sorted(expected_left.items(), key=lambda x: x[0]):
            actual = to_int(leftover.get(pid,0),0)
            report.append({
                "المنتج": (products_map.get(pid,{}) or {}).get("name", pid),
                "تحميل": to_int(loads.get(pid,0),0),
                "مباع (فواتير)": to_int(sold_total.get(pid,0),0),
                "المتوقع متبقي": exp,
                "المتبقي الفعلي": actual,
                "الفرق": exp - actual,
            })
        st.dataframe(report, use_container_width=True, hide_index=True)

        a,b,c = st.columns(3)
        a.metric("💵 المتوقع (نقد من الفواتير)", f"{cash_total:.3f}")
        b.metric("✅ المسلّم فعليًا", f"{cash_delivered:.3f}")
        c.metric("🔻 فرق النقد", f"{(cash_total - cash_delivered):.3f}")

        st.caption(f"أُغلق في: {_dt_short(close_doc.get('closed_at',''))} | بواسطة: {close_doc.get('closed_by','')}")
        return

    st.info("أدخل المتبقي فعليًا لكل صنف + النقد المسلّم. سيظهر الفرق مباشرة.")

    leftover_inputs = {}
    for pid, exp in sorted(expected_left.items(), key=lambda x: x[0]):
        nm = (products_map.get(pid,{}) or {}).get("name", pid)
        cols = st.columns([2.2, 1.1, 1.1])
        cols[0].markdown(f"**{nm}**  \nالمتوقع: `{exp}`")
        leftover_inputs[pid] = cols[1].number_input("المتبقي الفعلي", min_value=0, step=1, value=0, key=f"dd_left_{pid}")
        cols[2].markdown(f"الفرق: **{exp - to_int(leftover_inputs[pid],0)}**")

    cash_delivered = st.number_input("✅ النقد المسلّم فعليًا", min_value=0.0, step=0.5, value=0.0, key="dd_cash_delivered")
    st.caption(f"المتوقع من فواتير النقد: {cash_total:.3f} | فرق النقد: {(cash_total - float(cash_delivered)):.3f}")

    if st.button("🔒 حفظ وإغلاق اليوم", use_container_width=True, key="dd_save_close"):
        set_daily_close(close_doc_id, {
            "date": day.strftime("%Y-%m-%d"),
            "seller_username": seller_username,
            "leftover": {pid: to_int(q,0) for pid,q in leftover_inputs.items()},
            "cash_expected": float(cash_total),
            "cash_delivered": float(cash_delivered),
            "cash_diff": float(cash_total - float(cash_delivered)),
            "computed_expected_left": expected_left,
            "active": True,
            "closed_at": now_iso(),
            "closed_by": user.get("username",""),
            "locked": True,
        })
        st.success("تم إغلاق اليوم ✅")
        st.rerun()
    #انتهاء تاب الموزعين    _____________________________

def orders_archive_page(go, user):
    tabs = st.tabs(["🧾 الأرشيف (كما هو)", "🚚 محاسبة موزّع يومية (إضافة)"])

    with tabs[0]:
        st.markdown("<h2 style='text-align:center;'>📁 أرشيف الفواتير + إحصائيات</h2>", unsafe_allow_html=True)
        st.caption("يعرض الفواتير المُسلّمة (done) فقط. مع فلترة فترة + عميل اختياري + بحث اختياري.")
        st.divider()

        role = (user or {}).get("role") or ""
        if role and role not in ("admin", "owner", "superadmin"):
            st.error("هذه الصفحة للأدمن فقط.")
            return

        if st.button("⬅️ رجوع", key="arch_back"):
            go("orders_prep")

        st.markdown("### 🔎 فلاتر")
        today = datetime.now(TZ).date()

        st.session_state.setdefault("arch_from", today - timedelta(days=6))
        st.session_state.setdefault("arch_to", today)
        st.session_state.setdefault("arch_preset", None)


        preset = st.session_state.get("arch_preset")
        if preset == "today":
            st.session_state["arch_from"] = today
            st.session_state["arch_to"] = today
            st.session_state["arch_preset"] = None
        elif preset == "7d":
            st.session_state["arch_from"] = today - timedelta(days=6)
            st.session_state["arch_to"] = today
            st.session_state["arch_preset"] = None

        dist_accounts = get_distributors_list()
        dist_display = [f"{a['username']} — {a.get('name', a['username'])}" for a in dist_accounts]
        dist_map = {
            f"{a['username']} — {a.get('name', a['username'])}": a["username"]
            for a in dist_accounts
        }

        c1, c2, c3, c4 = st.columns([1.2, 1.2, 2.2, 2.0])
        
        with c1:
            d_from = st.date_input("من", key="arch_from")
        with c2:
            d_to = st.date_input("إلى", key="arch_to")
        with c3:
            invoice_search = st.text_input("بحث برقم الفاتورة (اختياري)", value="", key="arch_invoice_search").strip()
        with c4:
            pick_seller = st.selectbox(
                "الموزّع (اختياري)",
                options=["(الكل)"] + dist_display,
                key="arch_seller_pick"
            )

        seller_filter = "" if pick_seller == "(الكل)" else dist_map.get(pick_seller, "")
        if d_to < d_from:
            st.error("تاريخ (إلى) يجب أن يكون ≥ (من).")
            return

        customers = list(db.collection("customers").where("active", "==", True).limit(800).stream())
        cust_list = [{"id": d.id, **(d.to_dict() or {})} for d in customers]
        cust_map = {c.get("name", c["id"]): c["id"] for c in cust_list}
        cust_name = st.selectbox(
            "العميل (اختياري)",
            options=["(الكل)"] + list(cust_map.keys()),
            index=0,
            key="arch_customer_pick"
        )
        customer_id = "" if cust_name == "(الكل)" else cust_map.get(cust_name, "")

        def _set_today():
            st.session_state["arch_preset"] = "today"

        def _set_7d():
            st.session_state["arch_preset"] = "7d"

        def _clear_search():
            st.session_state["arch_invoice_search"] = ""

        b1, b2, b3 = st.columns(3)
        with b1:
            st.button("📌 اليوم", use_container_width=True, on_click=_set_today)
        with b2:
            st.button("📆 آخر 7 أيام", use_container_width=True, on_click=_set_7d)
        with b3:
            st.button("🧹 تصفير البحث", use_container_width=True, on_click=_clear_search)

        start_iso = _iso_start_of_day(d_from)
        end_iso = _iso_start_of_next_day(d_to)

        PAGE_SIZE = 50
        st.session_state.setdefault("arch_page", 1)
        st.session_state.setdefault("arch_last_doc", None)
        st.session_state.setdefault("arch_stack", [])

        sig = (start_iso, end_iso, customer_id, invoice_search, seller_filter)
        if st.session_state.get("arch_sig") != sig:
            st.session_state["arch_sig"] = sig
            st.session_state["arch_page"] = 1
            st.session_state["arch_last_doc"] = None
            st.session_state["arch_stack"] = []

        q = (
            db.collection("sales")
            .where("active", "==", True)
            .where("status", "==", "done")
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

        if (not invoice_search) and st.session_state.arch_last_doc is not None and st.session_state.arch_page > 1:
            q = q.start_after(st.session_state.arch_last_doc)

        docs = list(q.stream())
        rows = []
        for d in docs:
            x = d.to_dict() or {}
            x["id"] = d.id
            rows.append(x)

        last_doc = docs[-1] if docs else None

        st.markdown("### 📊 الإحصائيات (done فقط)")
        calc = st.button("⚡ احسب الإحصائيات", key="arch_calc_stats")

        st.session_state.setdefault("arch_stats_sig", None)
        st.session_state.setdefault("arch_stats", None)

        if calc:
            HARD_CAP = 8000
            sq = (
                db.collection("sales")
                .where("active", "==", True)
                .where("status", "==", "done")
                .limit(HARD_CAP)
            )

            if invoice_search:
                sq = sq.where("invoice_no", "==", invoice_search).limit(HARD_CAP)
            else:
                sq = sq.where("delivered_at", ">=", start_iso).where("delivered_at", "<", end_iso)
                if customer_id:
                    sq = sq.where("customer_id", "==", customer_id)
                if seller_filter:
                    sq = sq.where("seller_username", "==", seller_filter)
                sq = sq.limit(HARD_CAP)

            cnt = 0
            sum_total = sum_discount = sum_net = sum_paid = sum_unpaid = sum_extra = 0.0
            cash_cnt = credit_cnt = 0

            for d in sq.stream():
                cnt += 1
                sd = d.to_dict() or {}
                total = prep_to_float(sd.get("total", 0))
                disc = prep_to_float(sd.get("discount", 0))
                net = prep_to_float(sd.get("net", total - disc))
                paid = prep_to_float(sd.get("amount_paid", 0))
                unpaid = prep_to_float(sd.get("unpaid_debt", 0))
                extra = prep_to_float(sd.get("extra_credit", 0))

                sum_total += total
                sum_discount += disc
                sum_net += net
                sum_paid += paid
                sum_unpaid += unpaid
                sum_extra += extra

                p = sd.get("payment_type")
                if p == "cash":
                    cash_cnt += 1
                elif p == "credit":
                    credit_cnt += 1

            st.session_state["arch_stats_sig"] = sig
            st.session_state["arch_stats"] = {
                "cnt": cnt,
                "total": sum_total,
                "disc": sum_discount,
                "net": sum_net,
                "paid": sum_paid,
                "unpaid": sum_unpaid,
                "extra": sum_extra,
                "cash_cnt": cash_cnt,
                "credit_cnt": credit_cnt,
            }

        stats = st.session_state.arch_stats if st.session_state.arch_stats_sig == sig else None
        a, b, c, d = st.columns(4)
        e, f, g, h = st.columns(4)

        if stats:
            a.metric("عدد الفواتير", f"{stats['cnt']}")
            b.metric("الإجمالي", f"{stats['total']:.2f}")
            c.metric("الخصم", f"{stats['disc']:.2f}")
            d.metric("الصافي", f"{stats['net']:.2f}")
            e.metric("المدفوع (Cash)", f"{stats['paid']:.2f}")
            f.metric("متبقي ذمم", f"{stats['unpaid']:.2f}")
            g.metric("زيادة كرصد", f"{stats['extra']:.2f}")
            h.metric("نقدي/ذمم", f"{stats['cash_cnt']} / {stats['credit_cnt']}")
        else:
            a.metric("عدد الفواتير", "—")
            b.metric("الإجمالي", "—")
            c.metric("الخصم", "—")
            d.metric("الصافي", "—")
            e.metric("المدفوع", "—")
            f.metric("متبقي", "—")
            g.metric("زيادة", "—")
            h.metric("نقدي/ذمم", "—")

        st.divider()
        st.markdown("### 🧾 النتائج")

        p1, p2, p3 = st.columns([1, 1, 1])
        with p1:
            can_prev = st.session_state.arch_page > 1 and len(st.session_state.arch_stack) > 0 and (not invoice_search)
            if st.button("⬅️ السابق", disabled=not can_prev, use_container_width=True):
                st.session_state.arch_last_doc = st.session_state.arch_stack.pop()
                st.session_state.arch_page -= 1
                st.rerun()

        with p2:
            st.caption(f"الصفحة: {st.session_state.arch_page}")

        with p3:
            can_next = (len(docs) == PAGE_SIZE) and (not invoice_search)
            if st.button("التالي ➡️", disabled=not can_next, use_container_width=True):
                if st.session_state.arch_last_doc is not None:
                    st.session_state.arch_stack.append(st.session_state.arch_last_doc)
                st.session_state.arch_last_doc = last_doc
                st.session_state.arch_page += 1
                st.rerun()

        if not rows:
            st.info("لا توجد نتائج ضمن الفلاتر الحالية.")
            return







        df = pd.DataFrame([{
            "رقم": r.get("invoice_no") or r.get("ref") or r["id"],
            "تاريخ": _dt_short(r.get("delivered_at") or r.get("updated_at") or r.get("created_at")),
            "العميل": r.get("customer_name") or "—",
            "الدفع": "ذمم" if r.get("payment_type") == "credit" else ("نقدي" if r.get("payment_type") == "cash" else "—"),
            "الصافي": float(prep_to_float(r.get("net", 0))),
        } for r in rows])

        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True
        )
        csv_data = df.to_csv(index=False).encode("utf-8-sig")

        st.download_button(
            label="⬇️ تحميل CSV",
            data=csv_data,
            file_name=f"archive_{d_from}_{d_to}.csv",
            mime="text/csv",
            key="arch_download_csv"
        )
        
        st.session_state.setdefault("arch_print_sid", None)
        st.session_state.setdefault("arch_print_mode", "invoice")
        st.session_state.setdefault("arch_print_open", False)
        st.session_state.setdefault("arch_print_paper", "80mm")

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
                    st.session_state["arch_print_open"] = True
                    st.session_state["arch_print_paper"] = "80mm"
                    st.rerun()

            with b2:
                can_rec = (ptype == "cash") and (paid > 0)
                if st.button("🧾 قبض", use_container_width=True, key=f"arch_rec_{sid}", disabled=not can_rec):
                    st.session_state["arch_print_sid"] = sid
                    st.session_state["arch_print_mode"] = "receipt"
                    st.session_state["arch_print_open"] = True
                    st.session_state["arch_print_paper"] = "80mm"
                    st.rerun()

            st.divider()

        if st.session_state.get("arch_print_open") and st.session_state.get("arch_print_sid"):
            st.markdown("### 🖨️ معاينة الطباعة")

            top1, top2 = st.columns([1.4, 1])
            with top1:
                paper = st.selectbox(
                    "نوع الورق",
                    ["80mm", "a4"],
                    index=0 if st.session_state.get("arch_print_paper", "80mm") == "80mm" else 1,
                    key="arch_print_paper_select"
                )
                st.session_state["arch_print_paper"] = paper

            with top2:
                st.write("")
                st.write("")
                if st.button("❌ إغلاق المعاينة", use_container_width=True, key="arch_close_preview"):
                    st.session_state["arch_print_open"] = False
                    st.session_state["arch_print_sid"] = None
                    st.session_state["arch_print_mode"] = "invoice"
                    st.rerun()

            sid = st.session_state.get("arch_print_sid")
            mode = st.session_state.get("arch_print_mode", "invoice")

            sale = doc_get("sales", sid) or {}
            sale["id"] = sid

            cust_id = sale.get("customer_id") or ""
            customer = doc_get("customers", cust_id) if cust_id else {}

            if mode == "receipt":
                html = build_receipt_html(
                    sale,
                    customer=customer or {},
                    company_name="مخابز البوادي",
                    paper=paper
                )
            else:
                html = build_invoice_html(
                    sale,
                    customer=customer or {},
                    company_name="مخابز البوادي",
                    paper=paper
                )

            if paper == "a4":
                st.warning("معاينة A4 داخل الصفحة قد لا تظهر كاملة، لكن الطباعة الفعلية تكون من نافذة الطباعة.")

            show_print_html(html, height=1100)

    with tabs[1]:
        distributor_daily_tab(go, user)