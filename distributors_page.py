import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime, timezone, timedelta

from firebase_config import db
from firebase_admin import firestore


# ---------------------------
# Helpers
# ---------------------------
def get_products_cache(limit=400):
    # ✅ كاش خفيف: لا يجلب المنتجات إلا عند فتح صفحة الموزعين
    if "products_cache" not in st.session_state:
        docs = db.collection("products").where("active", "==", True).limit(limit).stream()
        items = []
        for d in docs:
            x = d.to_dict() or {}
            items.append({"id": d.id, **x})
        items.sort(key=lambda r: (r.get("name") or ""))
        st.session_state.products_cache = items
    return st.session_state.products_cache


def now_iso():
    tz = timezone(timedelta(hours=3))  # Jordan
    return datetime.now(tz).isoformat()


def to_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


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
        {"active": False, "updated_at": now_iso()}, merge=True
    )


def _money_int(x):
    try:
        return f"{int(x)}"
    except Exception:
        return "0"


def _money3(x):
    try:
        return f"{float(x):.3f}"
    except Exception:
        return "0.000"


# ---------------------------
# Crate moves queries
# ---------------------------
def _get_moves_for_dist(dist_id: str, limit=300):
    docs = db.collection("crate_moves").where("distributor_id", "==", dist_id).limit(limit).stream()
    out = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        out.append({"id": d.id, **x})
    out.sort(key=lambda r: (r.get("created_at") or ""))
    return out


def _build_dist_statement(dist: dict, moves: list):
    """
    ✅ كشف موحّد:
      - out/in/adjust للصناديق
      - cash للتحصيل النقدي
      - عرض الرصيد التراكمي للصناديق
      - عرض مبلغ العملية (إن وجد)
    """
    running_boxes = 0
    rows = []

    for m in moves:
        t = (m.get("created_at", "") or "")[:19].replace("T", " ")
        typ = (m.get("type") or "")
        note = m.get("note") or ""
        ref = f"MOVE:{m.get('id','')}"

        boxes_qty = to_int(m.get("boxes_qty", 0))
        delta_boxes = to_int(m.get("delta_boxes", 0))

        prod_name = (m.get("product_name") or "")
        units_per_box = to_int(m.get("units_per_box", 0))
        total_units = to_int(m.get("total_units", 0))

        unit_price = to_float(m.get("unit_price", 0))
        amount = to_float(m.get("amount", 0))

        # ---- صندوق/تحصيل ----
        delta = 0
        label = ""
        qty_show = 0

        if typ == "out":
            delta = +boxes_qty
            qty_show = boxes_qty
            label = "تسليم صناديق"
        elif typ == "in":
            delta = -boxes_qty
            qty_show = boxes_qty
            label = "استلام صناديق"
        elif typ == "cash":
            delta = 0
            qty_show = 0
            label = "تحصيل نقدي"
        else:  # adjust
            delta = delta_boxes
            qty_show = abs(delta_boxes)
            label = "تعديل صناديق"

        running_boxes += delta

        extra = ""
        if typ in ["out", "in"] and prod_name:
            extra = f" | المنتج: {prod_name} | محتوى الصندوق: {units_per_box} | الإجمالي: {total_units}"
        if typ in ["out", "in"] and unit_price > 0:
            extra += f" | سعر الوحدة: {unit_price:.3f}"
        if typ == "cash":
            extra = f" | مبلغ التحصيل: {amount:.3f}"

        rows.append({
            "التاريخ": t,
            "النوع": label,
            "الكمية": qty_show,
            "أثر": delta,
            "الرصيد": running_boxes,
            "المبلغ": (amount if typ in ["out", "in", "cash"] else ""),
            "ملاحظة": (note + extra).strip(),
            "مرجع": ref,
        })

    final_boxes = running_boxes
    return rows, final_boxes


# ---------------------------
# Printing HTML
# ---------------------------
def build_distributor_statement_html(dist: dict, rows: list, final_balance: int, company_name="نظام المخبز", paper="80mm"):
    name = dist.get("name") or dist.get("id") or "—"
    phone = dist.get("phone") or ""
    money_bal = to_float(dist.get("money_balance", 0))
    dt = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

    width_css = "280px" if paper == "80mm" else "820px"
    font_css = "12px" if paper == "80mm" else "14px"

    rows = rows[-120:] if rows else []

    body = ""
    for r in rows:
        amt = r.get("المبلغ", "")
        amt_show = _money3(amt) if amt != "" else ""
        body += f"""
        <tr>
          <td>{r.get("التاريخ","")}</td>
          <td>{r.get("النوع","")}</td>
          <td>{_money_int(r.get("الكمية",0))}</td>
          <td>{_money_int(r.get("أثر",0))}</td>
          <td><b>{_money_int(r.get("الرصيد",0))}</b></td>
          <td>{amt_show}</td>
        </tr>
        """

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Distributor Statement</title>
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
  .sig {{ margin-top: 14px; display:flex; justify-content:space-between; gap: 18px; }}
  .sig div {{ width: 50%; border-top:1px solid #ddd; padding-top:8px; }}
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
      <div style="margin-top:6px;font-weight:700;">كشف موزّع (صناديق + مبالغ)</div>
    </div>

    <hr/>

    <div class="sumrow"><span>الموزّع:</span><span><b>{name}</b></span></div>
    {f"<div class='sumrow'><span>هاتف:</span><span>{phone}</span></div>" if phone else ""}
    <div class="sumrow"><span>رصيد الصناديق:</span><span><b>{_money_int(final_balance)}</b></span></div>
    <div class="sumrow"><span>الرصيد المالي على الموزّع:</span><span><b>{money_bal:.3f}</b></span></div>
    <div class="sumrow"><span>تاريخ الطباعة:</span><span>{dt}</span></div>

    <hr/>

    <table>
      <thead>
        <tr>
          <th>تاريخ</th>
          <th>نوع</th>
          <th>كمية</th>
          <th>أثر</th>
          <th>الرصيد</th>
          <th>المبلغ</th>
        </tr>
      </thead>
      <tbody>
        {body if body else "<tr><td colspan='6' class='muted'>لا توجد حركات.</td></tr>"}
      </tbody>
    </table>

    <div class="sig">
      <div>توقيع المستلم</div>
      <div>توقيع المحاسب</div>
    </div>

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
# Transaction: apply move (crates + stock + money)
# ---------------------------
@firestore.transactional
def _tx_apply_move(transaction, dist_id: str, move_doc_id: str, move_data: dict):
    """
    ✅ يحدّث رصيد الصناديق داخل distributors
    ✅ يخصم/يرجع من مخزون المنتج حسب total_units (عند out/in)
    ✅ يحدّث الرصيد المالي money_balance حسب price * total_units (عند out/in)
    ✅ يسجل الحركة داخل crate_moves atomically
    """
    dist_ref = db.collection("distributors").document(dist_id)
    dist_snap = dist_ref.get(transaction=transaction)
    if not dist_snap.exists:
        raise ValueError("الموزّع غير موجود")

    dist = dist_snap.to_dict() or {}
    cur_boxes = to_int(dist.get("crates_balance", 0))
    cur_money = to_float(dist.get("money_balance", 0))

    typ = move_data.get("type")  # out | in | adjust
    boxes_qty = to_int(move_data.get("boxes_qty", 0))
    delta_boxes = 0

    # بيانات المخزن
    product_id = (move_data.get("product_id") or "").strip()
    units_per_box = to_int(move_data.get("units_per_box", 0))
    total_units = to_int(move_data.get("total_units", 0))

    # =========================
    # 1) أثر الصناديق
    # =========================
    if typ == "out":
        if boxes_qty <= 0:
            raise ValueError("عدد الصناديق يجب أن يكون أكبر من صفر")
        delta_boxes = +boxes_qty

    elif typ == "in":
        if boxes_qty <= 0:
            raise ValueError("عدد الصناديق يجب أن يكون أكبر من صفر")
        delta_boxes = -boxes_qty
        if cur_boxes + delta_boxes < 0:
            raise ValueError("لا يمكن أن يصبح رصيد الصناديق أقل من صفر")

    else:  # adjust
        delta_boxes = to_int(move_data.get("delta_boxes", 0))
        if delta_boxes == 0:
            raise ValueError("ضع قيمة تعديل للصناديق (موجب/سالب)")
        if cur_boxes + delta_boxes < 0:
            raise ValueError("لا يمكن أن يصبح رصيد الصناديق أقل من صفر")

    new_boxes_balance = cur_boxes + delta_boxes

    # =========================
    # 2) تحديث مخزون المنتج + الرصيد المالي (out/in فقط)
    # =========================
    if typ in ["out", "in"]:
        if not product_id:
            raise ValueError("اختر المنتج المرتبط بالصناديق")
        if units_per_box <= 0:
            raise ValueError("محتوى الصندوق يجب أن يكون أكبر من صفر")
        if total_units <= 0:
            raise ValueError("الكمية الإجمالية غير صحيحة")

        prod_ref = db.collection("products").document(product_id)
        prod_snap = prod_ref.get(transaction=transaction)
        if not prod_snap.exists:
            raise ValueError("المنتج غير موجود في المخزن")

        prod = prod_snap.to_dict() or {}
        cur_stock = to_float(prod.get("qty_on_hand", 0))

        unit_price = to_float(prod.get("price", 0))
        amount = float(total_units) * float(unit_price)

        # خزّنها داخل الحركة (مفيد للكشف)
        move_data["unit_price"] = float(unit_price)
        move_data["amount"] = float(amount)

        if typ == "out":
            if cur_stock < float(total_units):
                raise ValueError(f"المخزون غير كافي. المتوفر {cur_stock} والمطلوب {total_units}")
            transaction.update(prod_ref, {
                "qty_on_hand": cur_stock - float(total_units),
                "updated_at": now_iso()
            })
            cur_money += float(amount)
        else:  # in (مرتجع)
            transaction.update(prod_ref, {
                "qty_on_hand": cur_stock + float(total_units),
                "updated_at": now_iso()
            })
            cur_money -= float(amount)

    # =========================
    # 3) تحديث الموزّع + حفظ الحركة
    # =========================
    transaction.update(dist_ref, {
        "crates_balance": new_boxes_balance,
        "money_balance": float(cur_money),
        "updated_at": now_iso()
    })

    mv_ref = db.collection("crate_moves").document(move_doc_id)
    transaction.set(mv_ref, move_data, merge=True)

    return new_boxes_balance, float(cur_money)


# ---------------------------
# Transaction: cash collection (money only)
# ---------------------------
@firestore.transactional
def _tx_apply_cash_collection(transaction, dist_id: str, move_doc_id: str, payload: dict):
    dist_ref = db.collection("distributors").document(dist_id)
    dist_snap = dist_ref.get(transaction=transaction)
    if not dist_snap.exists:
        raise ValueError("الموزّع غير موجود")

    dist = dist_snap.to_dict() or {}
    cur_money = to_float(dist.get("money_balance", 0))

    amount = to_float(payload.get("amount", 0))
    if amount <= 0:
        raise ValueError("مبلغ التحصيل يجب أن يكون أكبر من صفر")

    new_money = cur_money - float(amount)

    transaction.update(dist_ref, {"money_balance": float(new_money), "updated_at": now_iso()})
    mv_ref = db.collection("crate_moves").document(move_doc_id)
    transaction.set(mv_ref, payload, merge=True)

    return float(new_money)


# ---------------------------
# Page UI
# ---------------------------
def distributors_page(go, user):
    st.markdown("<h2 style='text-align:center;'>🚚 الموزّعين (عهدة الصناديق + تحصيل نقدي)</h2>", unsafe_allow_html=True)
    st.caption("تسليم/استلام صناديق + رصيد صناديق + رصيد مالي + تحصيل نقدي + كشف + طباعة")
    st.divider()

    top_left, _, _ = st.columns([1, 2, 1])
    with top_left:
        if st.button("⬅️ رجوع للوحة التحكم", key="back_to_dashboard_distributors"):
            go("dashboard")

    tabs = st.tabs(["👤 إدارة الموزّعين", "📦 حركة + تحصيل", "📄 كشف وطباعة"])

    # ---------------------------
    # Tab 1: Manage
    # ---------------------------
    with tabs[0]:
        st.subheader("👤 إدارة الموزّعين")

        with st.expander("➕ إضافة موزّع", expanded=False):
            with st.form("add_distributor_form"):
                name = st.text_input("اسم الموزّع *")
                phone = st.text_input("الهاتف (اختياري)")
                submit = st.form_submit_button("حفظ")

            if submit:
                if not name.strip():
                    st.error("اسم الموزّع مطلوب")
                else:
                    dist_id = name.strip().lower().replace(" ", "_")
                    doc_set("distributors", dist_id, {
                        "name": name.strip(),
                        "phone": phone.strip(),
                        "crates_balance": 0,
                        "money_balance": 0.0,  # ✅ جديد
                        "active": True,
                        "created_at": now_iso(),
                        "updated_at": now_iso(),
                        "created_by": user.get("username", ""),
                    }, merge=True)
                    st.success("تمت إضافة الموزّع ✅")
                    st.rerun()

        q = st.text_input("🔎 بحث موزّع", placeholder="اكتب اسم/هاتف...", key="dist_search")
        dists = col_to_list("distributors", where_active=True)
        if q.strip():
            qq = q.strip().lower()
            dists = [d for d in dists if qq in ((d.get("name","") + " " + d.get("phone","") + " " + d.get("id","")).lower())]

        st.markdown("### قائمة الموزّعين")
        if not dists:
            st.info("لا يوجد موزّعين بعد.")
        else:
            rows = []
            for d in sorted(dists, key=lambda x: (x.get("name") or "")):
                rows.append({
                    "id": d["id"],
                    "name": d.get("name",""),
                    "phone": d.get("phone",""),
                    "crates_balance": to_int(d.get("crates_balance", 0)),
                    "money_balance": round(to_float(d.get("money_balance", 0)), 3),
                })

            st.dataframe(rows, use_container_width=True, hide_index=True)

            st.divider()
            del_id = st.selectbox("🗑️ تعطيل موزّع", options=[""] + [d["id"] for d in dists], key="dist_del_select")
            if st.button("تعطيل", use_container_width=True, key="dist_del_btn"):
                if del_id:
                    doc_soft_delete("distributors", del_id)
                    st.success("تم تعطيل الموزّع ✅")
                    st.rerun()

    # ---------------------------
    # Tab 2: Moves + Cash
    # ---------------------------
    with tabs[1]:
        st.subheader("📦 حركة الصناديق + 💵 تحصيل نقدي")

        dists = col_to_list("distributors", where_active=True)
        if not dists:
            st.info("أضف موزّعين أولًا.")
        else:
            dist_map = {d.get("name", d["id"]): d["id"] for d in dists}
            dist_by_id = {d["id"]: d for d in dists}

            sel_name = st.selectbox("اختر الموزّع", options=[""] + list(dist_map.keys()), key="move_dist_select")
            if not sel_name:
                st.info("اختر موزّع.")
            else:
                dist_id = dist_map[sel_name]
                dist = dist_by_id.get(dist_id, {"id": dist_id})

                bal_boxes = to_int(dist.get("crates_balance", 0))
                bal_money = to_float(dist.get("money_balance", 0))

                m1, m2 = st.columns(2)
                m1.metric("🧺 رصيد الصناديق", f"{bal_boxes}")
                m2.metric("💰 الرصيد المالي على الموزّع", f"{bal_money:.3f}")

                st.divider()

                # ✅ منتجات (كاش خفيف)
                products = get_products_cache()
                prod_map = {p.get("name", p["id"]): p["id"] for p in products}
                prod_by_id = {p["id"]: p for p in products}

                typ = st.selectbox(
                    "نوع الحركة",
                    ["out", "in", "adjust"],
                    format_func=lambda x: {
                        "out": "تسليم صناديق (خصم من المخزن + يزيد الرصيد المالي)",
                        "in": "استلام صناديق (إرجاع للمخزن + ينقص الرصيد المالي)",
                        "adjust": "تعديل صناديق فقط (لا يؤثر على المخزن/المبلغ)"
                    }[x],
                    key="move_type"
                )

                if typ in ["out", "in"]:
                    prod_name = st.selectbox(
                        "المنتج داخل الصناديق",
                        options=[""] + list(prod_map.keys()),
                        key="move_product"
                    )

                    boxes_qty = st.number_input(
                        "عدد الصناديق",
                        min_value=0, step=1, value=0,
                        key="move_boxes_qty"
                    )

                    units_per_box = st.number_input(
                        "محتوى الصندوق (كم قطعة داخل الصندوق)",
                        min_value=1, step=1, value=12,
                        key="move_units_per_box"
                    )

                    total_units = int(boxes_qty) * int(units_per_box)
                    st.caption(f"سيتم {'خصم' if typ=='out' else 'إرجاع'} كمية: **{total_units}** من المخزن")

                    # عرض مبلغ تقديري حسب سعر المنتج
                    product_id_preview = prod_map.get(prod_name, "")
                    unit_price_preview = to_float((prod_by_id.get(product_id_preview, {}) or {}).get("price", 0))
                    est_amount = float(total_units) * float(unit_price_preview)
                    if prod_name:
                        st.info(f"💰 السعر للوحدة: **{unit_price_preview:.3f}** | مبلغ العملية: **{est_amount:.3f}**")

                    delta_boxes = 0

                else:
                    prod_name = ""
                    boxes_qty = 0
                    units_per_box = 0
                    total_units = 0

                    delta_boxes = st.number_input(
                        "تعديل الصناديق (+/-)",
                        value=0, step=1,
                        key="move_delta_boxes"
                    )

                note = st.text_input(
                    "ملاحظة (اختياري)",
                    placeholder="تحميل صباح / رجوع / كسر ...",
                    key="move_note"
                )

                if st.button("✅ حفظ الحركة", use_container_width=True, key="move_save_btn"):
                    try:
                        if typ in ["out", "in"] and not prod_name:
                            st.error("اختر المنتج")
                            st.stop()

                        product_id = prod_map.get(prod_name, "") if prod_name else ""
                        product = prod_by_id.get(product_id, {}) if product_id else {}

                        move_doc_id = db.collection("crate_moves").document().id

                        payload = {
                            "distributor_id": dist_id,
                            "distributor_name": dist.get("name", ""),
                            "type": typ,

                            "boxes_qty": int(boxes_qty) if typ in ["out", "in"] else 0,
                            "delta_boxes": int(delta_boxes) if typ == "adjust" else 0,

                            "product_id": product_id,
                            "product_name": product.get("name", "") if product_id else "",
                            "units_per_box": int(units_per_box),
                            "total_units": int(total_units),

                            "note": (note or "").strip(),
                            "status": "done",
                            "created_at": now_iso(),
                            "updated_at": now_iso(),
                            "created_by": user.get("username", ""),
                            "active": True,
                        }

                        new_boxes, new_money = _tx_apply_move(db.transaction(), dist_id, move_doc_id, payload)
                        st.success(f"تم حفظ الحركة ✅ | رصيد الصناديق: {new_boxes} | الرصيد المالي: {new_money:.3f}")
                        st.rerun()

                    except Exception as e:
                        st.error(f"فشل حفظ الحركة: {e}")

                # ---------------------------
                # Cash collection
                # ---------------------------
                st.divider()
                st.markdown("### 💵 تحصيل نقدي من الموزّع (ينقص الرصيد المالي)")

                cash_amount = st.number_input(
                    "مبلغ التحصيل (نقدي)",
                    min_value=0.0, step=0.5, value=0.0,
                    key="dist_cash_amount"
                )
                cash_note = st.text_input(
                    "ملاحظة (اختياري)",
                    key="dist_cash_note",
                    placeholder="تحصيل اليوم / دفعة ..."
                )

                if st.button("✅ تسجيل تحصيل نقدي", use_container_width=True, key="dist_cash_save"):
                    try:
                        move_doc_id = db.collection("crate_moves").document().id
                        payload = {
                            "distributor_id": dist_id,
                            "distributor_name": dist.get("name", ""),
                            "type": "cash",
                            "amount": float(cash_amount),
                            "note": (cash_note or "").strip(),
                            "status": "done",
                            "created_at": now_iso(),
                            "updated_at": now_iso(),
                            "created_by": user.get("username", ""),
                            "active": True,
                        }
                        new_money = _tx_apply_cash_collection(db.transaction(), dist_id, move_doc_id, payload)
                        st.success(f"تم تسجيل التحصيل ✅ | الرصيد المالي الجديد: {new_money:.3f}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"فشل التحصيل: {e}")

                # ---------------------------
                # Last moves
                # ---------------------------
                st.divider()
                st.markdown("### آخر 25 حركة (صناديق/تحصيل)")
                moves = _get_moves_for_dist(dist_id, limit=250)
                tail = moves[-25:] if moves else []

                view = []
                for m in tail:
                    t = (m.get("created_at", "") or "")[:19].replace("T", " ")
                    typm = m.get("type")

                    boxes = to_int(m.get("boxes_qty", 0))
                    dbox = to_int(m.get("delta_boxes", 0))
                    pname = m.get("product_name", "")
                    total_u = to_int(m.get("total_units", 0))
                    amt = to_float(m.get("amount", 0))

                    if typm == "out":
                        label = "تسليم"
                        eff = +boxes
                        extra = f"{pname} | إجمالي: {total_u} | مبلغ: {amt:.3f}" if pname else ""
                    elif typm == "in":
                        label = "استلام"
                        eff = -boxes
                        extra = f"{pname} | إجمالي: {total_u} | مبلغ: {amt:.3f}" if pname else ""
                    elif typm == "cash":
                        label = "تحصيل نقدي"
                        eff = 0
                        extra = f"مبلغ: {amt:.3f}"
                    else:
                        label = "تعديل"
                        eff = dbox
                        extra = ""

                    view.append({
                        "التاريخ": t,
                        "النوع": label,
                        "أثر (صناديق)": eff,
                        "مبلغ": (f"{amt:.3f}" if typm in ["out", "in", "cash"] else ""),
                        "ملاحظة": (m.get("note", "") + ((" | " + extra) if extra else "")).strip(),
                    })

                if view:
                    st.dataframe(view, use_container_width=True, hide_index=True)
                else:
                    st.info("لا توجد حركات بعد.")

    # ---------------------------
    # Tab 3: Statement + Print
    # ---------------------------
    with tabs[2]:
        st.subheader("📄 كشف موزّع + طباعة")

        dists = col_to_list("distributors", where_active=True)
        if not dists:
            st.info("أضف موزّعين أولًا.")
            return

        dist_map = {d.get("name", d["id"]): d["id"] for d in dists}
        dist_by_id = {d["id"]: d for d in dists}

        sel_name = st.selectbox("اختر الموزّع", options=[""] + list(dist_map.keys()), key="stmt_dist_select")
        if not sel_name:
            st.info("اختر موزّع لعرض الكشف.")
            return

        dist_id = dist_map[sel_name]
        dist = dist_by_id.get(dist_id, {"id": dist_id})

        moves = _get_moves_for_dist(dist_id, limit=800)
        rows, final_balance = _build_dist_statement(dist, moves)

        s1, s2, s3 = st.columns(3)
        s1.metric("🧺 رصيد الصناديق", f"{final_balance}")
        s2.metric("💰 الرصيد المالي", f"{to_float(dist.get('money_balance', 0)):.3f}")
        s3.metric("عدد الحركات", f"{len(moves)}")

        p1, p2 = st.columns([1.2, 2.8])
        with p1:
            paper = st.selectbox("ورق الطباعة", ["80mm", "a4"], index=0, key="dist_stmt_paper")
        with p2:
            if st.button("🖨️ طباعة الكشف", use_container_width=True, key="dist_stmt_print"):
                html = build_distributor_statement_html(
                    dist=dist,
                    rows=rows,
                    final_balance=final_balance,
                    company_name="نظام المخبز",
                    paper=paper
                )
                show_print_html(html, height=820)

        st.divider()
        st.markdown("### جدول الحركات")
        st.dataframe(rows[-250:], use_container_width=True, hide_index=True)
