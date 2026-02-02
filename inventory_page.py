import streamlit as st
from datetime import datetime
from firebase_config import db
from firebase_admin import firestore
import time

# ---------------------------
# Helpers
# ---------------------------
def now_iso():
    return datetime.utcnow().isoformat()

def to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def to_int(x, default=0):
    try:
        return int(x)
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
    db.collection(collection).document(doc_id).set({"active": False, "updated_at": now_iso()}, merge=True)

def write_stock_move(move: dict):
    move["created_at"] = now_iso()
    move["active"] = True
    db.collection("stock_moves").add(move)


# ---------------------------
# Tab 1: Materials
# ---------------------------
def tab_materials(user):
    st.subheader("📦 المواد الخام")

    with st.expander("➕ إضافة مادة خام", expanded=False):
        with st.form("add_material"):
            name = st.text_input("اسم المادة", key="mat_add_name")
            unit = st.selectbox("الوحدة", ["kg", "g", "L", "pcs", "box"], index=0, key="mat_add_unit")
            qty = st.number_input("الكمية الحالية", min_value=0.0, step=0.1, key="mat_add_qty")
            min_qty = st.number_input("حد إعادة الطلب (Min)", min_value=0.0, step=0.1, key="mat_add_min")
            last_cost = st.number_input("آخر تكلفة شراء (اختياري)", min_value=0.0, step=0.01, key="mat_add_cost")
            submitted = st.form_submit_button("حفظ")

        if submitted:
            if not name.strip():
                st.error("اسم المادة مطلوب")
            else:
                doc_id = name.strip().lower().replace(" ", "_")
                doc_set("materials", doc_id, {
                    "name": name.strip(),
                    "unit": unit,
                    "qty_on_hand": float(qty),
                    "min_qty": float(min_qty),
                    "last_cost": float(last_cost),
                    "active": True,
                    "created_at": now_iso(),
                    "updated_at": now_iso(),
                })
                st.success("تمت إضافة المادة ✅")
                st.rerun()

    q = st.text_input("🔎 بحث", placeholder="اكتب اسم مادة...", key="mat_search")
    materials = col_to_list("materials", where_active=True)
    if q.strip():
        qq = q.strip().lower()
        materials = [m for m in materials if qq in (m.get("name", "").lower() + " " + m.get("id", "").lower())]

    low = [m for m in materials if to_float(m.get("qty_on_hand")) <= to_float(m.get("min_qty")) and to_float(m.get("min_qty")) > 0]
    if low:
        st.warning("⚠️ مواد تحت حد إعادة الطلب:")
        for m in low[:10]:
            st.write(f"- {m.get('name','')} ({m.get('qty_on_hand',0)} {m.get('unit','')})")

    st.markdown("### قائمة المواد الخام")
    if not materials:
        st.info("لا يوجد مواد حتى الآن.")
        return

    rows = []
    for m in sorted(materials, key=lambda x: x.get("name", "")):
        rows.append({
            "id": m["id"],
            "name": m.get("name", ""),
            "unit": m.get("unit", ""),
            "qty_on_hand": to_float(m.get("qty_on_hand")),
            "min_qty": to_float(m.get("min_qty")),
            "last_cost": to_float(m.get("last_cost")),
        })

    edited = st.data_editor(
        rows,
        use_container_width=True,
        hide_index=True,
        disabled=["id", "name", "unit"],
        column_config={
            "qty_on_hand": st.column_config.NumberColumn("الكمية الحالية", step=0.1),
            "min_qty": st.column_config.NumberColumn("حد إعادة الطلب", step=0.1),
            "last_cost": st.column_config.NumberColumn("آخر تكلفة", step=0.01),
        },
        key="materials_editor"
    )

    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 حفظ التعديلات على المواد", use_container_width=True, key="mat_save_btn"):
            for r in edited:
                doc_set("materials", r["id"], {
                    "qty_on_hand": float(r["qty_on_hand"]),
                    "min_qty": float(r["min_qty"]),
                    "last_cost": float(r["last_cost"]),
                    "updated_at": now_iso(),
                }, merge=True)
            st.success("تم حفظ التعديلات ✅")
            st.rerun()

    with colB:
        del_id = st.selectbox("🗑️ تعطيل مادة (حذف منطقي)", options=[""] + [m["id"] for m in materials], key="mat_del_select")
        if st.button("حذف مادة", use_container_width=True, key="mat_del_btn"):
            if del_id:
                doc_soft_delete("materials", del_id)
                st.success("تم تعطيل المادة ✅")
                st.rerun()

    st.divider()
    st.markdown("### ➕/➖ تعديل يدوي لمادة (يسجل حركة مخزون)")
    mat_opts = {m["name"]: m["id"] for m in materials if m.get("name")}
    mat_name = st.selectbox("المادة", options=[""] + list(mat_opts.keys()), key="mat_adj_select")
    delta = st.number_input("التغيير (+ إضافة / - خصم)", value=0.0, step=0.1, key="mat_adj_delta")
    note = st.text_input("ملاحظة", placeholder="سبب التعديل: تلف، تصحيح، ...", key="mat_adj_note")

    if st.button("تطبيق التعديل على المادة", use_container_width=True, key="mat_adj_btn"):
        if not mat_name:
            st.error("اختر مادة")
            return
        if delta == 0:
            st.error("ضع قيمة للتغيير")
            return

        mat_id = mat_opts[mat_name]

        @firestore.transactional
        def tx_update(transaction):
            ref = db.collection("materials").document(mat_id)
            snap = ref.get(transaction=transaction)
            data = snap.to_dict() or {}
            cur = to_float(data.get("qty_on_hand", 0))
            new_qty = cur + float(delta)
            if new_qty < 0:
                raise ValueError("لا يمكن أن تصبح الكمية أقل من صفر")
            transaction.update(ref, {"qty_on_hand": new_qty, "updated_at": now_iso()})

        try:
            tx_update(db.transaction())
            m = doc_get("materials", mat_id) or {}
            write_stock_move({
                "type": "adjustment",
                "ref_type": "manual",
                "ref_id": mat_id,
                "item_type": "material",
                "item_id": mat_id,
                "item_name": m.get("name", ""),
                "qty_delta": float(delta),
                "unit": m.get("unit", ""),
                "note": note.strip(),
                "created_by": user.get("username", ""),
            })
            st.success("تم التعديل وتسجيل الحركة ✅")
            st.rerun()
        except Exception as e:
            st.error(f"فشل التعديل: {e}")


# ---------------------------
# Tab 2: Products
# ---------------------------
def tab_products(user):
    st.subheader("🥖 المنتجات")

    with st.expander("➕ إضافة منتج", expanded=False):
        with st.form("add_product"):
            name = st.text_input("اسم المنتج", key="prod_add_name")
            sale_unit = st.selectbox("وحدة البيع", ["pcs", "kg", "box", "tray"], index=0, key="prod_add_unit")
            qty = st.number_input("الكمية الحالية (منتج جاهز)", min_value=0.0, step=1.0, key="prod_add_qty")
            price = st.number_input("سعر البيع ", min_value=0.0, step=0.05, key="prod_add_price")
            submitted = st.form_submit_button("حفظ")
        if submitted:
            if not name.strip():
                st.error("اسم المنتج مطلوب")
            else:
                doc_id = name.strip().lower().replace(" ", "_")
                doc_set("products", doc_id, {
                    "name": name.strip(),
                    "sale_unit": sale_unit,
                    "qty_on_hand": float(qty),
                    "price": float(price),
                    "active": True,
                    "created_at": now_iso(),
                    "updated_at": now_iso(),
                })
                st.success("تمت إضافة المنتج ✅")
                st.rerun()

    q = st.text_input("🔎 بحث", placeholder="اكتب اسم منتج...", key="prod_search")
    products = col_to_list("products", where_active=True)
    if q.strip():
        qq = q.strip().lower()
        products = [p for p in products if qq in (p.get("name", "").lower() + " " + p.get("id", "").lower())]

    st.markdown("### قائمة المنتجات")
    if not products:
        st.info("لا يوجد منتجات حتى الآن.")
        return

    rows = []
    for p in sorted(products, key=lambda x: x.get("name", "")):
        rows.append({
            "id": p["id"],
            "name": p.get("name", ""),
            "sale_unit": p.get("sale_unit", ""),
            "qty_on_hand": to_float(p.get("qty_on_hand")),
            "price": to_float(p.get("price")),
        })

    edited = st.data_editor(
        rows,
        use_container_width=True,
        hide_index=True,
        disabled=["id", "name", "sale_unit"],
        column_config={
            "qty_on_hand": st.column_config.NumberColumn("المخزون", step=1.0),
            "price": st.column_config.NumberColumn("السعر", step=0.05),
        },
        key="products_editor"
    )

    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 حفظ التعديلات على المنتجات", use_container_width=True, key="prod_save_btn"):
            for r in edited:
                doc_set("products", r["id"], {
                    "qty_on_hand": float(r["qty_on_hand"]),
                    "price": float(r["price"]),
                    "updated_at": now_iso(),
                }, merge=True)
            st.success("تم حفظ التعديلات ✅")
            st.rerun()

    with colB:
        del_id = st.selectbox("🗑️ حذف منتج", options=[""] + [p["id"] for p in products], key="prod_del_select")
        if st.button("حذف المنتج", use_container_width=True, key="prod_del_btn"):
            if del_id:
                doc_soft_delete("products", del_id)
                st.success("تم حذف المنتج ✅")
                st.rerun()


# ---------------------------
# Tab 3: BOMs
# ---------------------------
def tab_boms(user):
    st.subheader("🧾 الوصفات (BoM)")

    products = col_to_list("products", where_active=True)
    materials = col_to_list("materials", where_active=True)

    if not products or not materials:
        st.info("لازم تضيف منتجات ومواد خام أولًا.")
        return

    prod_map = {p["name"]: p["id"] for p in products}
    mat_map = {m["name"]: m["id"] for m in materials}

    prod_name = st.selectbox("اختر المنتج", options=[""] + list(prod_map.keys()), key="bom_prod_select")
    if not prod_name:
        st.info("اختر منتج لعرض/تعديل وصفته.")
        return

    prod_id = prod_map[prod_name]
    bom_doc = doc_get("boms", prod_id)
    items = (bom_doc.get("items", []) if bom_doc else [])

    st.markdown("### مكونات الوصفة الحالية")
    if items:
        st.dataframe(items, use_container_width=True, hide_index=True)
    else:
        st.warning("لا يوجد وصفة لهذا المنتج بعد.")

    st.divider()
    st.markdown("### ➕ إضافة مكون للوصفة")
    mat_name = st.selectbox("المادة الخام", options=[""] + list(mat_map.keys()), key="bom_mat_select")
    qty_per_unit = st.number_input("الكمية لكل وحدة منتج", min_value=0.0, step=0.01, key="bom_qty_per")
    note = st.text_input("ملاحظة (اختياري)", placeholder="مثال: طحين قوي/ماء/...", key="bom_note")

    if st.button("إضافة المكوّن", use_container_width=True, key="bom_add_btn"):
        if not mat_name:
            st.error("اختر مادة")
            return
        if qty_per_unit <= 0:
            st.error("الكمية يجب أن تكون أكبر من صفر")
            return

        mat_id = mat_map[mat_name]
        m = doc_get("materials", mat_id) or {}

        new_items = [i for i in items if i.get("material_id") != mat_id]
        new_items.append({
            "material_id": mat_id,
            "material_name": m.get("name", ""),
            "qty_per_unit": float(qty_per_unit),
            "unit": m.get("unit", ""),
            "note": note.strip()
        })

        doc_set("boms", prod_id, {
            "product_id": prod_id,
            "product_name": prod_name,
            "items": new_items,
            "active": True,
            "updated_at": now_iso(),
            "updated_by": user.get("username", ""),
        }, merge=True)

        st.success("تم تحديث الوصفة ✅")
        st.rerun()

    st.markdown("### 🗑️ حذف مكوّن من الوصفة")
    if items:
        names = [f"{i.get('material_name')} ({i.get('qty_per_unit')} {i.get('unit')})" for i in items]
        idx = st.selectbox("اختر مكوّن للحذف", options=[""] + names, key="bom_del_select")
        if st.button("حذف المكوّن", use_container_width=True, key="bom_del_btn"):
            if idx:
                rm_i = names.index(idx)
                new_items = items[:rm_i] + items[rm_i+1:]
                doc_set("boms", prod_id, {
                    "items": new_items,
                    "updated_at": now_iso(),
                    "updated_by": user.get("username", ""),
                }, merge=True)
                st.success("تم الحذف ✅")
                st.rerun()


# ---------------------------
# Tab 4: Production Orders
# ---------------------------
def tab_production_orders(user):
    st.subheader("🏭 أوامر الإنتاج")

    products = col_to_list("products", where_active=True)
    if not products:
        st.info("أضف منتجات أولًا.")
        return

    prod_map = {p["name"]: p["id"] for p in products}

    st.markdown("### ✅ تسجيل أمر إنتاج (ويخصم المواد من الوصفة تلقائيًا)")

    prod_name = st.selectbox("المنتج المراد إنتاجه", options=[""] + list(prod_map.keys()), key="po_prod_select")
    qty = st.number_input("الكمية المنتجة", min_value=0.0, step=1.0, key="po_qty_input")
    date = st.date_input("تاريخ الإنتاج", key="po_date_input")
    note = st.text_input("ملاحظة (اختياري)", key="po_note_input")

    if st.button("تسجيل أمر الإنتاج", use_container_width=True, key="po_submit_btn"):
        if not prod_name:
            st.error("اختر منتج")
            return
        if qty <= 0:
            st.error("الكمية يجب أن تكون أكبر من صفر")
            return

        prod_id = prod_map[prod_name]
        bom = doc_get("boms", prod_id)
        if not bom or not bom.get("items"):
            st.error("لا توجد وصفة (BoM) لهذا المنتج. أنشئ الوصفة أولًا.")
            return

        bom_items = bom["items"]
        qty_produced = float(qty)

        @firestore.transactional
        def tx_create(transaction):
            # قراءة المواد
            mat_refs, mat_snaps = [], []
            for it in bom_items:
                ref = db.collection("materials").document(it["material_id"])
                snap = ref.get(transaction=transaction)
                if not snap.exists:
                    raise ValueError(f"مادة غير موجودة: {it.get('material_name','')}")
                mat_refs.append(ref)
                mat_snaps.append(snap)

            # تحقق مخزون
            required = []
            for it, snap in zip(bom_items, mat_snaps):
                cur = to_float((snap.to_dict() or {}).get("qty_on_hand", 0))
                req = float(it["qty_per_unit"]) * qty_produced
                if cur < req:
                    raise ValueError(f"المخزون غير كافي للمادة: {it.get('material_name')} (المطلوب {req}, المتوفر {cur})")
                required.append(req)

            # خصم المواد
            for ref, snap, req in zip(mat_refs, mat_snaps, required):
                cur = to_float((snap.to_dict() or {}).get("qty_on_hand", 0))
                transaction.update(ref, {"qty_on_hand": cur - req, "updated_at": now_iso()})

            # إضافة المنتج الجاهز
            prod_ref = db.collection("products").document(prod_id)
            prod_snap = prod_ref.get(transaction=transaction)
            cur_p = to_float((prod_snap.to_dict() or {}).get("qty_on_hand", 0))
            transaction.update(prod_ref, {"qty_on_hand": cur_p + qty_produced, "updated_at": now_iso()})

            # إنشاء أمر الإنتاج
            po_ref = db.collection("production_orders").document()
            transaction.set(po_ref, {
                "product_id": prod_id,
                "product_name": prod_name,
                "qty_produced": qty_produced,
                "date": str(date),
                "status": "done",
                "note": note.strip(),
                "created_by": user.get("username", ""),
                "created_at": now_iso(),
                "active": True,
            })
            return po_ref.id

        try:
            po_id = tx_create(db.transaction())

            # حركات مخزون: استهلاك مواد
            for it in bom_items:
                req = float(it["qty_per_unit"]) * qty_produced
                write_stock_move({
                    "type": "production_consume",
                    "ref_type": "production_order",
                    "ref_id": po_id,
                    "item_type": "material",
                    "item_id": it["material_id"],
                    "item_name": it.get("material_name", ""),
                    "qty_delta": -req,
                    "unit": it.get("unit", ""),
                    "note": f"استهلاك إنتاج {prod_name}",
                    "created_by": user.get("username", ""),
                })

            # حركة مخزون: إنتاج منتج
            p = doc_get("products", prod_id) or {}
            write_stock_move({
                "type": "production_produce",
                "ref_type": "production_order",
                "ref_id": po_id,
                "item_type": "product",
                "item_id": prod_id,
                "item_name": prod_name,
                "qty_delta": qty_produced,
                "unit": p.get("sale_unit", "pcs"),
                "note": "إضافة مخزون منتج بعد الإنتاج",
                "created_by": user.get("username", ""),
            })

            st.success(f"تم تسجيل أمر الإنتاج ✅ (ID: {po_id})")
            st.rerun()

        except Exception as e:
            st.error(f"فشل تسجيل أمر الإنتاج: {e}")

    st.divider()
    st.markdown("### سجل أوامر الإنتاج (آخر 30)")

    try:
        docs = db.collection("production_orders") \
            .order_by("created_at", direction=firestore.Query.DESCENDING) \
            .limit(50) \
            .stream()
    except Exception:
        docs = db.collection("production_orders").limit(50).stream()

    rows = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        rows.append({
            "id": d.id,
            "date": x.get("date", ""),
            "product": x.get("product_name", ""),
            "qty": x.get("qty_produced", 0),
            "status": x.get("status", ""),
            "by": x.get("created_by", ""),
            "note": x.get("note", ""),
        })

    rows = rows[:30]
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("لا يوجد أوامر إنتاج بعد.")


# ---------------------------
# Tab 5: Inventory Count (الجرد)
# ---------------------------

def _new_count_header(scope: str, user: dict, note: str):
    ref = db.collection("inventory_counts").document()
    ref.set({
        "scope": scope,          # materials | products | both
        "status": "draft",       # draft | posted
        "note": note.strip(),
        "created_by": user.get("username", ""),
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "active": True,
    }, merge=True)
    return ref.id

def _list_recent_counts(limit=20):
    try:
        q = db.collection("inventory_counts") \
            .order_by("created_at", direction=firestore.Query.DESCENDING) \
            .limit(limit)
        docs = q.stream()
    except Exception:
        docs = db.collection("inventory_counts").limit(limit).stream()

    rows = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        rows.append({
            "id": d.id,
            "scope": x.get("scope", ""),
            "status": x.get("status", ""),
            "created_at": x.get("created_at", ""),
            "created_by": x.get("created_by", ""),
            "note": x.get("note", ""),
        })
    return rows

def _line_doc_id(count_id: str, item_type: str, item_id: str):
    return f"{count_id}__{item_type}__{item_id}"

def _upsert_count_lines_from_system(count_id: str, scope: str):
    mats = col_to_list("materials", where_active=True) if scope in ("materials", "both") else []
    prods = col_to_list("products", where_active=True) if scope in ("products", "both") else []

    batch = db.batch()
    op = 0

    def commit_if_needed():
        nonlocal batch, op
        if op >= 400:
            batch.commit()
            batch = db.batch()
            op = 0

    for m in mats:
        doc_id = _line_doc_id(count_id, "material", m["id"])
        ref = db.collection("inventory_count_lines").document(doc_id)
        batch.set(ref, {
            "count_id": count_id,
            "item_type": "material",
            "item_id": m["id"],
            "item_name": m.get("name", ""),
            "unit": m.get("unit", ""),
            "system_qty": to_float(m.get("qty_on_hand", 0)),
            "counted_qty": None,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "active": True,
        }, merge=True)
        op += 1
        commit_if_needed()

    for p in prods:
        doc_id = _line_doc_id(count_id, "product", p["id"])
        ref = db.collection("inventory_count_lines").document(doc_id)
        batch.set(ref, {
            "count_id": count_id,
            "item_type": "product",
            "item_id": p["id"],
            "item_name": p.get("name", ""),
            "unit": p.get("sale_unit", "pcs"),
            "system_qty": to_float(p.get("qty_on_hand", 0)),
            "counted_qty": None,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "active": True,
        }, merge=True)
        op += 1
        commit_if_needed()

    if op > 0:
        batch.commit()

def _get_count_lines(count_id: str):
    docs = db.collection("inventory_count_lines").where("count_id", "==", count_id).stream()

    rows = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        rows.append({
            "doc_id": d.id,
            "item_type": x.get("item_type", ""),
            "item_id": x.get("item_id", ""),
            "item_name": x.get("item_name", ""),
            "unit": x.get("unit", ""),
            "system_qty": to_float(x.get("system_qty", 0)),
            "counted_qty": x.get("counted_qty", None),
        })

    rows.sort(key=lambda r: (r["item_type"], r["item_name"]))
    return rows

def _save_counted_lines(edited_rows):
    batch = db.batch()
    op = 0

    def commit_if_needed():
        nonlocal batch, op
        if op >= 400:
            batch.commit()
            batch = db.batch()
            op = 0

    for r in edited_rows:
        ref = db.collection("inventory_count_lines").document(r["doc_id"])
        cq = r.get("counted_qty", None)
        if cq is None or cq == "":
            cq_val = None
        else:
            cq_val = float(cq)

        batch.update(ref, {"counted_qty": cq_val, "updated_at": now_iso()})
        op += 1
        commit_if_needed()

    if op > 0:
        batch.commit()

def _post_inventory_count(count_id: str, user: dict):
    header_ref = db.collection("inventory_counts").document(count_id)
    header = header_ref.get().to_dict() or {}
    if header.get("status") != "draft":
        raise ValueError("لا يمكن ترحيل جرد غير مسودة.")

    lines = _get_count_lines(count_id)
    lines_to_post = [l for l in lines if l.get("counted_qty") is not None]

    if not lines_to_post:
        raise ValueError("لا يوجد كميات معدودة لترحيلها. أدخل counted_qty أولاً.")

    batch = db.batch()
    op = 0

    def commit_if_needed():
        nonlocal batch, op
        if op >= 350:
            batch.commit()
            batch = db.batch()
            op = 0

    for l in lines_to_post:
        counted = float(l["counted_qty"])
        if counted < 0:
            raise ValueError(f"لا يمكن إدخال كمية سالبة: {l['item_name']}")

        system_qty = float(l["system_qty"])
        delta = counted - system_qty
        if abs(delta) < 1e-12:
            continue

        if l["item_type"] == "material":
            ref = db.collection("materials").document(l["item_id"])
            batch.update(ref, {"qty_on_hand": counted, "updated_at": now_iso()})
        else:
            ref = db.collection("products").document(l["item_id"])
            batch.update(ref, {"qty_on_hand": counted, "updated_at": now_iso()})

        op += 1
        commit_if_needed()

        write_stock_move({
            "type": "count",
            "ref_type": "inventory_count",
            "ref_id": count_id,
            "item_type": l["item_type"],
            "item_id": l["item_id"],
            "item_name": l["item_name"],
            "qty_delta": float(delta),
            "unit": l.get("unit", ""),
            "note": "ترحيل جرد",
            "created_by": user.get("username", ""),
        })

    header_ref.set({
        "status": "posted",
        "posted_at": now_iso(),
        "posted_by": user.get("username", ""),
        "updated_at": now_iso(),
    }, merge=True)

    if op > 0:
        batch.commit()

def tab_inventory_count(user):
    st.subheader("🧮 الجرد (بسيط للبائع)")
    st.caption("الخطوات: 1) ابدأ جرد  2) حمّل الأصناف  3) أدخل المعدود  4) اعتمد الجرد لتحديث المخزون")
    st.divider()

    # =========================
    # 1) بدء جرد جديد
    # =========================
    c1, c2 = st.columns([2, 1])

    with c1:
        scope = st.selectbox(
            "اختر نوع الجرد",
            ["materials", "products", "both"],
            format_func=lambda x: {"materials": "مواد خام فقط", "products": "منتجات فقط", "both": "مواد + منتجات"}[x],
            key="ic_scope_simple"
        )
        note = st.text_input("ملاحظة (اختياري)", key="ic_note_simple", placeholder="مثال: جرد نهاية اليوم / جرد أسبوعي...")

    with c2:
        if st.button("➕ ابدأ جرد جديد", use_container_width=True, key="ic_new_btn_simple"):
            cid = _new_count_header(scope, user, note)
            st.session_state["ic_selected"] = cid
            st.success(f"تم إنشاء جرد جديد ✅ رقم: {cid}")
            st.rerun()

    st.divider()

    # =========================
    # 2) اختيار جرد (آخر الجردات)
    # =========================
    st.markdown("### 📋 اختر جرد للعمل عليه")
    counts = _list_recent_counts(limit=20)
    if not counts:
        st.info("لا يوجد جردات بعد. اضغط (ابدأ جرد جديد).")
        return

    # جهّز خيارات عرض واضحة للبائع
    options = [""] + [c["id"] for c in counts]

    def _nice_label(cid: str):
        if cid == "":
            return "— اختر —"
        c = next((x for x in counts if x["id"] == cid), None)
        if not c:
            return cid

        status = (c.get("status") or "")
        status_txt = "✅ مُعتمد" if status == "posted" else "📝 قيد العمل"
        scope_txt = {"materials": "مواد", "products": "منتجات", "both": "مواد+منتجات"}.get(c.get("scope", ""), "")
        created_at = (c.get("created_at", "") or "")[:19].replace("T", " ")
        return f"{status_txt} | {scope_txt} | {created_at} | {cid}"

    selected = st.selectbox(
        "الجردات الأخيرة",
        options=options,
        format_func=_nice_label,
        key="ic_select_simple"
    )

    if not selected:
        st.info("اختر جرد من القائمة.")
        return

    header_ref = db.collection("inventory_counts").document(selected)
    header = header_ref.get().to_dict() or {}
    status = header.get("status", "draft")
    scope_sel = header.get("scope", "materials")

    # عرض معلومات بسيطة
    st.markdown("### ℹ️ معلومات الجرد")
    status_txt = "✅ مُعتمد (انتهى)" if status == "posted" else "📝 قيد العمل"
    st.write(f"**الحالة:** {status_txt}")
    st.write(f"**النوع:** { {'materials':'مواد خام','products':'منتجات','both':'مواد + منتجات'}.get(scope_sel, scope_sel) }")
    if header.get("note"):
        st.write(f"**ملاحظة:** {header.get('note','')}")

    if status == "posted":
        st.warning("هذا الجرد مُعتمد ولا يمكن تعديله. إذا بدك تعدّل اعمل جرد جديد.")
        return

    st.divider()

    # =========================
    # 3) تحميل الأصناف
    # =========================
    st.markdown("### 1️⃣ تحميل قائمة الأصناف للجرد")
    st.caption("اضغط مرة واحدة لتحميل الأصناف من النظام (مثل ورقة جرد جاهزة).")

    if st.button("📥 تحميل/تحديث قائمة الأصناف", use_container_width=True, key="ic_load_btn_simple"):
        _upsert_count_lines_from_system(selected, scope_sel)
        header_ref.set({"updated_at": now_iso()}, merge=True)
        st.success("تم تحميل الأصناف ✅")
        st.rerun()

    st.divider()

    # =========================
    # 4) إدخال المعدود (ورقة جرد)
    # =========================
    st.markdown("### 2️⃣ أدخل الكمية المعدودة")
    st.caption("اكتب فقط (الكمية المعدودة). الفرق يظهر تلقائياً.")

    lines = _get_count_lines(selected)
    if not lines:
        st.info("لا توجد أصناف بعد. اضغط (تحميل/تحديث قائمة الأصناف) أولاً.")
        return

    # فلترة للبحث (مهم للبائع)
    q = st.text_input("🔎 بحث سريع", key="ic_search_simple", placeholder="اكتب اسم الصنف...")
    qq = q.strip().lower()

    table_rows = []
    for l in lines:
        name = (l.get("item_name") or "")
        if qq and qq not in name.lower():
            continue

        system_qty = float(l.get("system_qty") or 0.0)
        cq = l.get("counted_qty", None)

        # فرق ظاهري (فقط للعرض)
        diff = ""
        if cq is not None and cq != "":
            try:
                diff = float(cq) - system_qty
            except Exception:
                diff = ""

        table_rows.append({
            "doc_id": l["doc_id"],
            "نوع": "مادة" if l["item_type"] == "material" else "منتج",
            "الصنف": name,
            "الوحدة": l.get("unit", ""),
            "بالنظام": system_qty,
            "المعدود": cq,
            "الفرق": diff if diff != "" else "",
        })

    if not table_rows:
        st.info("لا يوجد نتائج للبحث.")
        return

    edited = st.data_editor(
        table_rows,
        use_container_width=True,
        hide_index=True,
        disabled=["doc_id", "نوع", "الصنف", "الوحدة", "بالنظام", "الفرق"],
        column_config={
            "بالنظام": st.column_config.NumberColumn("بالنظام", step=0.1),
            "المعدود": st.column_config.NumberColumn("المعدود", step=0.1),
            "الفرق": st.column_config.NumberColumn("الفرق", step=0.1),
        },
        key="ic_editor_simple"
    )

    st.divider()

    # =========================
    # 5) أزرار بسيطة
    # =========================
    b1, b2 = st.columns(2)

    with b1:
        if st.button("💾 حفظ (بدون اعتماد)", use_container_width=True, key="ic_save_btn_simple"):
            to_save = [{"doc_id": r["doc_id"], "counted_qty": r.get("المعدود", None)} for r in edited]
            _save_counted_lines(to_save)
            header_ref.set({"updated_at": now_iso()}, merge=True)
            st.success("تم الحفظ ✅ (يمكنك المتابعة لاحقاً)")
            st.rerun()

    with b2:
        st.warning("اعتماد الجرد يحدّث المخزون مباشرة.")
        if st.button("✅ اعتماد الجرد وتحديث المخزون", use_container_width=True, key="ic_post_btn_simple"):
            try:
                # احفظ أولاً قبل الاعتماد
                to_save = [{"doc_id": r["doc_id"], "counted_qty": r.get("المعدود", None)} for r in edited]
                _save_counted_lines(to_save)

                _post_inventory_count(selected, user)
                st.success("تم اعتماد الجرد وتحديث المخزون ✅ وتم تسجيل حركة (count)")
                st.rerun()
            except Exception as e:
                st.error(f"فشل اعتماد الجرد: {e}")

# ---------------------------
# Tab 6: Stock Moves
# ---------------------------
def tab_stock_moves():
    st.subheader("🔁 حركة المخزون")

    type_filter = st.selectbox(
        "فلترة حسب النوع",
        options=["الكل", "purchase", "sale", "production_consume", "production_produce", "adjustment", "count"],
        index=0,
        key="moves_type_filter"
    )

    limit = st.selectbox(
        "عدد السجلات",
        options=[50, 100, 200],
        index=0,
        key="moves_limit"
    )

    try:
        q = db.collection("stock_moves") \
            .order_by("created_at", direction=firestore.Query.DESCENDING) \
            .limit(int(limit))
        docs = q.stream()
    except Exception:
        docs = db.collection("stock_moves").limit(int(limit)).stream()

    rows = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        if type_filter != "الكل" and x.get("type") != type_filter:
            continue

        rows.append({
            "created_at": x.get("created_at", ""),
            "type": x.get("type", ""),
            "item_type": x.get("item_type", ""),
            "item_name": x.get("item_name", ""),
            "qty_delta": x.get("qty_delta", 0),
            "unit": x.get("unit", ""),
            "ref_type": x.get("ref_type", ""),
            "ref_id": x.get("ref_id", ""),
            "by": x.get("created_by", ""),
            "note": x.get("note", ""),
        })

    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("لا يوجد حركات.")


# ---------------------------
# Tab 7: Projection
# ---------------------------
def tab_projection():
    st.subheader("⚠️ توقع الإنتاج (Projection)")

    products = col_to_list("products", where_active=True)
    materials = col_to_list("materials", where_active=True)

    if not products or not materials:
        st.info("أضف منتجات ومواد خام أولًا.")
        return

    prod_map = {p["name"]: p["id"] for p in products}

    prod_name = st.selectbox(
        "اختر المنتج",
        options=[""] + list(prod_map.keys()),
        key="projection_product_select"
    )

    target = st.number_input(
        "هدف إنتاج (اختياري)",
        min_value=0.0,
        step=1.0,
        value=0.0,
        key="projection_target_qty"
    )

    if not prod_name:
        st.info("اختر منتج لعرض التوقع.")
        return

    prod_id = prod_map[prod_name]
    bom = doc_get("boms", prod_id)

    if not bom or not bom.get("items"):
        st.error("لا توجد وصفة (BoM) لهذا المنتج.")
        return

    mat_by_id = {m["id"]: m for m in materials}

    rows = []
    max_by_each = []
    bottleneck = None
    bottleneck_value = None

    for it in bom["items"]:
        mat_id = it["material_id"]
        m = mat_by_id.get(mat_id)
        if not m:
            continue

        stock = to_float(m.get("qty_on_hand"))
        per = to_float(it.get("qty_per_unit"))
        if per <= 0:
            continue

        possible = stock / per
        max_by_each.append(possible)

        if bottleneck_value is None or possible < bottleneck_value:
            bottleneck_value = possible
            bottleneck = it.get("material_name", "")

        needed_for_target = per * float(target) if target > 0 else 0.0
        shortage = max(0.0, needed_for_target - stock) if target > 0 else 0.0

        rows.append({
            "المادة": it.get("material_name", ""),
            "المتوفر": stock,
            "الوحدة": m.get("unit", ""),
            "لكل وحدة منتج": per,
            "أقصى إنتاج من هذه المادة": round(possible, 2),
            "المطلوب للهدف": round(needed_for_target, 2) if target > 0 else "",
            "النقص للهدف": round(shortage, 2) if target > 0 else "",
        })

    if not rows:
        st.warning("لا توجد مواد في الوصفة.")
        return

    max_qty = min(max_by_each) if max_by_each else 0.0

    col1, col2 = st.columns(2)
    col1.metric("الإنتاج الممكن الآن", f"{max_qty:.2f}")
    col2.metric("المادة المُقيّدة (Bottleneck)", bottleneck or "-")

    st.divider()
    st.dataframe(rows, use_container_width=True, hide_index=True)

    if target > 0:
        st.divider()
        enough = all((r["النقص للهدف"] == "" or r["النقص للهدف"] == 0.0) for r in rows)
        if enough:
            st.success("✅ المخزون الحالي يكفي لتحقيق هدف الإنتاج.")
        else:
            st.warning("⚠️ المخزون لا يكفي لتحقيق الهدف. راجع عمود (النقص للهدف).")


# ---------------------------
# Tabs UI (IMPORTANT: must be at the end)
# ---------------------------
def inventory_page(go, user):
    st.markdown("<h2 style='text-align:center;'>📦 إدارة المستودع</h2>", unsafe_allow_html=True)
    st.caption("مواد + منتجات + BoM + إنتاج + جرد + حركات + توقع إنتاج")
    st.divider()

    top_left, _, _ = st.columns([1, 2, 1])
    with top_left:
        if st.button("⬅️ رجوع للوحة التحكم", key="back_to_dashboard"):
            go("dashboard")

    tabs = st.tabs([
        "📦 المواد الخام",
        "🥖 المنتجات",
        "🧾 الوصفات (BoM)",
        "🏭 أوامر الإنتاج",
        "🧮 الجرد (Inventory Count)",
        "🔁 حركة المخزون",
        "⚠️ توقع الإنتاج (Projection)"
    ])

    with tabs[0]:
        tab_materials(user)

    with tabs[1]:
        tab_products(user)

    with tabs[2]:
        tab_boms(user)

    with tabs[3]:
        tab_production_orders(user)

    with tabs[4]:
        tab_inventory_count(user)

    with tabs[5]:
        tab_stock_moves()

    with tabs[6]:
        tab_projection()
