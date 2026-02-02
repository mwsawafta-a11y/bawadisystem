# orders_prep_page.py
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime, timezone, timedelta
from firebase_config import db
from firebase_admin import firestore


# ---------------------------
# Helpers
# ---------------------------
def now_iso():
    tz = timezone(timedelta(hours=3))  # Jordan
    return datetime.now(tz).isoformat()

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

def write_stock_move(move: dict):
    move["created_at"] = now_iso()
    move["active"] = True
    db.collection("stock_moves").add(move)

def _supports_dialog():
    return hasattr(st, "dialog")


# ---------------------------
# UI helpers (Free qty + Card)
# ---------------------------
def _toggle_free_qty(pid: str):
    k = f"show_free_qty__{pid}"
    st.session_state[k] = not bool(st.session_state.get(k, False))

def _apply_free_qty(pid: str, stock_int: int):
    """
    يقرأ قيمة الإدخال من widget ثم يطبّقها على السلة
    """
    qty_key = f"free_qty__{pid}"
    raw = st.session_state.get(qty_key, 0)
    q = int(to_int(raw, 0))

    # منع التجاوز + منع السالب
    if q < 0:
        q = 0
    if q > int(stock_int):
        q = int(stock_int)

    if q == 0:
        st.session_state.prep_cart.pop(pid, None)
    else:
        st.session_state.prep_cart[pid] = q

    # تزامن الإدخال (داخل callback = آمن)
    st.session_state[qty_key] = q

def _set_cart_qty(pid: str, qty: int, stock_int: int):
    """
    تحديث السلة من أزرار +1/+5/-/0 مع احترام المخزون
    """
    q = int(to_int(qty, 0))
    if q < 0:
        q = 0
    if q > int(stock_int):
        q = int(stock_int)

    if q == 0:
        st.session_state.prep_cart.pop(pid, None)
    else:
        st.session_state.prep_cart[pid] = q

    # لو كان حقل الكمية الحرة موجود، خليه يتزامن
    qty_key = f"free_qty__{pid}"
    if qty_key in st.session_state:
        st.session_state[qty_key] = q

def _clear_prep_cart_and_free_qty_keys():
    """
    تفريغ السلة + تصفير كل حقول الكمية الحرة
    """
    st.session_state.prep_cart = {}
    for k in list(st.session_state.keys()):
        if k.startswith("free_qty__") or k.startswith("show_free_qty__"):
            st.session_state.pop(k, None)


# ---------------------------
# Customer special prices (hidden)
# ---------------------------
def _get_customer_prices_map(customer_id: str, limit=400):
    """
    Returns { product_id: price_float }
    """
    if not customer_id:
        return {}

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
            out[pid] = float(to_float(x.get("price", 0.0)))
    return out


# ---------------------------
# Printing (HTML)
# ---------------------------
def _money(x):
    try:
        return f"{float(x):.3f}"
    except Exception:
        return "0.000"

def _dt_short(x):
    return (x or "")[:19].replace("T", " ")

def build_invoice_html(sale: dict, customer: dict = None, company_name="نظام المخبز", paper="80mm"):
    """
    Invoice shows items and totals.
    payment_type:
      - "credit" => ذمم
      - "cash"   => دفع
    plus:
      - amount_paid
      - extra_credit
      - unpaid_debt
    """
    customer = customer or {}
    items = sale.get("items", []) or []

    created = _dt_short(sale.get("created_at") or sale.get("updated_at"))
    delivered = _dt_short(sale.get("delivered_at"))
    dt = delivered if delivered else created
    if not dt:
        dt = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

    invoice_no = sale.get("invoice_no") or sale.get("ref") or sale.get("id") or ""
    cust_name = sale.get("customer_name") or customer.get("name") or "—"
    phone = customer.get("phone", "")

    total = to_float(sale.get("total", 0))
    discount = to_float(sale.get("discount", 0))
    net = float(total) - float(discount)

    ptype = sale.get("payment_type")
    paid = float(to_float(sale.get("amount_paid", 0)))
    extra_credit = float(to_float(sale.get("extra_credit", 0)))
    unpaid = float(to_float(sale.get("unpaid_debt", 0)))

    if ptype == "credit":
        header_type = "فاتورة ذمم - آجل"
    elif ptype == "cash":
        if unpaid > 0:
            header_type = "فاتورة دفع جزئي + ذمم متبقي"
        else:
            header_type = "فاتورة نقدي - مدفوعة"
    else:
        header_type = "فاتورة (غير محدد الدفع)"

    rows_html = ""
    for it in items:
        pname = it.get("product_name") or "-"
        qty = to_float(it.get("qty", 0))
        price = to_float(it.get("price", 0))
        line_total = to_float(it.get("total")) if it.get("total") is not None else (qty * price)
        rows_html += f"""
          <tr>
            <td class="name">{pname}</td>
            <td class="qty">{_money(qty)}</td>
            <td class="price">{_money(price)}</td>
            <td class="tot">{_money(line_total)}</td>
          </tr>
        """

    width_css = "800px" if paper == "a4" else "280px"
    font_css = "14px" if paper == "a4" else "12px"

    pay_lines = ""
    if ptype == "cash":
        pay_lines += f"<div class='sumrow'><span>المدفوع:</span><span><b>{_money(paid)}</b></span></div>"
        if extra_credit > 0:
            pay_lines += f"<div class='sumrow'><span>زيادة كرصد للعميل:</span><span><b>{_money(extra_credit)}</b></span></div>"
        if unpaid > 0:
            pay_lines += f"<div class='sumrow'><span>متبقي ذمم:</span><span><b>{_money(unpaid)}</b></span></div>"

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Invoice</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; }}
  .wrap {{ width: {width_css}; margin: 0 auto; padding: 12px; font-size: {font_css}; }}
  .center {{ text-align: center; }}
  .muted {{ color: #666; }}
  hr {{ border: none; border-top: 1px dashed #999; margin: 10px 0; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 6px 2px; vertical-align: top; }}
  th {{ border-bottom: 1px solid #ddd; text-align: right; }}
  td.name {{ width: 46%; }}
  td.qty  {{ width: 14%; text-align: right; }}
  td.price{{ width: 20%; text-align: right; }}
  td.tot  {{ width: 20%; text-align: right; }}
  .sumrow {{ display: flex; justify-content: space-between; margin-top: 6px; }}
  .btnbar {{ margin: 10px 0 0 0; display: flex; gap: 8px; }}
  button {{ padding: 10px 12px; cursor: pointer; width: 100%; }}
  .badge {{
    display: inline-block; padding: 6px 10px; border: 1px solid #ccc; border-radius: 999px;
    margin-top: 6px; font-weight: 700;
  }}
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
      <div class="badge">{header_type}</div>
    </div>

    <hr/>

    <div>
      <div class="sumrow"><span>رقم الفاتورة:</span><span><b>{invoice_no}</b></span></div>
      <div class="sumrow"><span>التاريخ:</span><span>{dt}</span></div>
      <div class="sumrow"><span>العميل:</span><span>{cust_name}</span></div>
      {("<div class='sumrow'><span>هاتف:</span><span>"+phone+"</span></div>") if phone else ""}
    </div>

    <hr/>

    <table>
      <thead>
        <tr>
          <th>الصنف</th>
          <th>كمية</th>
          <th>سعر</th>
          <th>الإجمالي</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>

    <hr/>

    <div class="sumrow"><span>الإجمالي:</span><span><b>{_money(total)}</b></span></div>
    <div class="sumrow"><span>خصم:</span><span><b>{_money(discount)}</b></span></div>
    <div class="sumrow" style="font-size:16px;"><span>الصافي:</span><span><b>{_money(net)}</b></span></div>

    {pay_lines}

    <hr/>
    <div class="center muted">شكراً لزيارتكم ❤️</div>

    <div class="btnbar">
      <button onclick="window.print()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html

def build_receipt_html(sale: dict, customer: dict = None, company_name="نظام المخبز", paper="80mm"):
    """
    Receipt for cash payments (or partial):
    Shows: paid, net, extra_credit OR unpaid_debt.
    """
    customer = customer or {}

    created = _dt_short(sale.get("created_at") or sale.get("updated_at"))
    delivered = _dt_short(sale.get("delivered_at"))
    dt = delivered if delivered else created
    if not dt:
        dt = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

    invoice_no = sale.get("invoice_no") or sale.get("ref") or sale.get("id") or ""
    cust_name = sale.get("customer_name") or customer.get("name") or "—"

    total = to_float(sale.get("total", 0))
    discount = to_float(sale.get("discount", 0))
    net = float(total) - float(discount)

    paid = float(to_float(sale.get("amount_paid", 0)))
    extra_credit = float(to_float(sale.get("extra_credit", 0)))
    unpaid = float(to_float(sale.get("unpaid_debt", 0)))

    width_css = "800px" if paper == "a4" else "280px"
    font_css = "14px" if paper == "a4" else "12px"

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Receipt</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; }}
  .wrap {{ width: {width_css}; margin: 0 auto; padding: 12px; font-size: {font_css}; }}
  .center {{ text-align: center; }}
  .muted {{ color: #666; }}
  hr {{ border: none; border-top: 1px dashed #999; margin: 10px 0; }}
  .sumrow {{ display: flex; justify-content: space-between; margin-top: 6px; }}
  .btnbar {{ margin: 10px 0 0 0; display: flex; gap: 8px; }}
  button {{ padding: 10px 12px; cursor: pointer; width: 100%; }}
  .badge {{
    display: inline-block; padding: 6px 10px; border: 1px solid #ccc; border-radius: 999px;
    margin-top: 6px; font-weight: 700;
  }}
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
      <div class="badge">إيصال قبض</div>
    </div>

    <hr/>

    <div class="sumrow"><span>رقم الفاتورة:</span><span><b>{invoice_no}</b></span></div>
    <div class="sumrow"><span>التاريخ:</span><span>{dt}</span></div>
    <div class="sumrow"><span>العميل:</span><span>{cust_name}</span></div>

    <hr/>

    <div class="sumrow"><span>صافي الفاتورة:</span><span><b>{_money(net)}</b></span></div>
    <div class="sumrow"><span>المبلغ المستلم:</span><span><b>{_money(paid)}</b></span></div>

    {f"<div class='sumrow'><span>زيادة كرصد للعميل:</span><span><b>{_money(extra_credit)}</b></span></div>" if extra_credit>0 else ""}
    {f"<div class='sumrow'><span>متبقي ذمم:</span><span><b>{_money(unpaid)}</b></span></div>" if unpaid>0 else ""}

    <hr/>
    <div class="center muted">هذا الإيصال يثبت عملية الدفع/الرصيد.</div>

    <div class="btnbar">
      <button onclick="window.print()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html



def build_debt_only_invoice_html(customer: dict, company_name="نظام المخبز", paper="80mm"):
    """
    فاتورة ذمم فقط: تعتمد على customer.balance
    - إذا balance > 0 => ذمم مستحقة (على العميل)
    - إذا balance <= 0 => لا يوجد ذمم (تطبع ورقة توضيح فقط)
    """
    customer = customer or {}
    cust_name = customer.get("name") or "—"
    phone = customer.get("phone") or ""

    bal = float(to_float(customer.get("balance", 0)))
    debt = bal if bal > 0 else 0.0

    dt = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

    width_css = "800px" if paper == "a4" else "280px"
    font_css = "14px" if paper == "a4" else "12px"

    msg = ""
    if bal <= 0:
        if bal < 0:
            msg = f"للعميل رصيد عندك: {_money(abs(bal))} (ليس ذمم)"
        else:
            msg = "لا يوجد ذمم مستحقة على العميل"

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Debt Only Invoice</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; }}
  .wrap {{ width: {width_css}; margin: 0 auto; padding: 12px; font-size: {font_css}; }}
  .center {{ text-align: center; }}
  .muted {{ color: #666; }}
  hr {{ border: none; border-top: 1px dashed #999; margin: 10px 0; }}
  .sumrow {{ display: flex; justify-content: space-between; margin-top: 6px; }}
  .btnbar {{ margin: 10px 0 0 0; display: flex; gap: 8px; }}
  button {{ padding: 10px 12px; cursor: pointer; width: 100%; }}
  .badge {{
    display: inline-block; padding: 6px 10px; border: 1px solid #ccc; border-radius: 999px;
    margin-top: 6px; font-weight: 700;
  }}
  table {{ width: 100%; border-collapse: collapse; margin-top: 8px; }}
  th, td {{ padding: 8px 4px; border-bottom: 1px solid #eee; text-align: right; }}
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
      <div class="badge">فاتورة ذمم فقط</div>
    </div>

    <hr/>

    <div class="sumrow"><span>التاريخ:</span><span>{dt}</span></div>
    <div class="sumrow"><span>العميل:</span><span><b>{cust_name}</b></span></div>
    {("<div class='sumrow'><span>هاتف:</span><span>"+phone+"</span></div>") if phone else ""}

    <hr/>

    <table>
      <thead>
        <tr>
          <th>البند</th>
          <th>المبلغ</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td><b>ذمم مستحقة على العميل</b></td>
          <td><b>{_money(debt)}</b></td>
        </tr>
      </tbody>
    </table>

    {f"<hr/><div class='center muted'>{msg}</div>" if msg else ""}

    <div class="btnbar">
      <button onclick="window.print()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html

# ---------------------------
# NEW: Customer statement (كشف حساب مختصر)
# ---------------------------
def _pick_dt_for_sort(s: dict):
    return (s.get("delivered_at") or s.get("updated_at") or s.get("created_at") or "")

def _calc_balance_delta_from_sale(s: dict) -> float:
    """
    نفس منطق tx_deliver:
    - credit: +net
    - cash  : +unpaid_debt - extra_credit
    """
    ptype = s.get("payment_type")
    net = float(to_float(s.get("net", 0)))
    unpaid = float(to_float(s.get("unpaid_debt", 0)))
    extra = float(to_float(s.get("extra_credit", 0)))

    if ptype == "credit":
        return +net
    if ptype == "cash":
        return (unpaid - extra)
    return 0.0

def _get_customer_sales_for_statement(customer_id: str, limit=200):
    """
    يجلب آخر الحركات من sales للعميل (prepared/done) ثم نفرز محلياً
    """
    if not customer_id:
        return []
    docs = (
        db.collection("sales")
        .where("customer_id", "==", customer_id)
        .limit(int(limit))
        .stream()
    )
    out = []
    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        x["id"] = d.id
        out.append(x)
    out.sort(key=lambda x: _pick_dt_for_sort(x), reverse=True)
    return out

def build_customer_statement_html(customer: dict, sales: list, company_name="نظام المخبز", paper="80mm", max_rows=30):
    """
    كشف حساب مختصر:
    - الرصيد الحالي
    - آخر الحركات (فواتير مسلّمة/محضّرة) مع: صافي/مدفوع/متبقي/زيادة/أثر على الرصيد
    """
    customer = customer or {}
    cust_name = customer.get("name") or "—"
    phone = customer.get("phone") or ""
    balance_now = float(to_float(customer.get("balance", 0)))

    width_css = "800px" if paper == "a4" else "280px"
    font_css = "14px" if paper == "a4" else "12px"

    rows = ""
    shown = 0

    for s in (sales or []):
        if shown >= int(max_rows):
            break

        inv = s.get("invoice_no") or s.get("ref") or s.get("id") or ""
        dt = _dt_short(_pick_dt_for_sort(s))
        status = s.get("status") or ""
        ptype = s.get("payment_type")

        net = float(to_float(s.get("net", 0)))
        paid = float(to_float(s.get("amount_paid", 0)))
        unpaid = float(to_float(s.get("unpaid_debt", 0)))
        extra = float(to_float(s.get("extra_credit", 0)))
        delta = float(_calc_balance_delta_from_sale(s))

        ptxt = "ذمم" if ptype == "credit" else ("نقدي" if ptype == "cash" else "—")
        stxt = "مُسلّم" if status == "done" else ("مُحضّر" if status == "prepared" else status)

        rows += f"""
          <tr>
            <td class="dt">{dt}</td>
            <td class="inv">{inv}</td>
            <td class="st">{stxt}</td>
            <td class="pt">{ptxt}</td>
            <td class="n">{_money(net)}</td>
            <td class="p">{_money(paid)}</td>
            <td class="u">{_money(unpaid)}</td>
            <td class="e">{_money(extra)}</td>
            <td class="d">{_money(delta)}</td>
          </tr>
        """
        shown += 1

    bal_label = "على العميل" if balance_now > 0 else ("للعميل رصيد" if balance_now < 0 else "الرصيد صفر")
    bal_value = _money(abs(balance_now))

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
  .sumrow {{ display: flex; justify-content: space-between; margin-top: 6px; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 6px 3px; vertical-align: top; text-align: right; }}
  th {{ border-bottom: 1px solid #ddd; }}
  td.inv {{ font-weight: 700; }}
  .badge {{
    display: inline-block; padding: 6px 10px; border: 1px solid #ccc; border-radius: 999px;
    margin-top: 6px; font-weight: 700;
  }}
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
      <div class="badge">كشف حساب عميل (مختصر)</div>
    </div>

    <hr/>

    <div class="sumrow"><span>العميل:</span><span><b>{cust_name}</b></span></div>
    {f"<div class='sumrow'><span>هاتف:</span><span>{phone}</span></div>" if phone else ""}
    <div class="sumrow"><span>الرصيد الحالي:</span><span><b>{bal_label}: {bal_value}</b></span></div>
    <div class="sumrow"><span>تاريخ الطباعة:</span><span>{datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")}</span></div>

    <hr/>

    <table>
      <thead>
        <tr>
          <th>تاريخ</th>
          <th>رقم</th>
          <th>حالة</th>
          <th>الدفع</th>
          <th>الصافي</th>
          <th>مدفوع</th>
          <th>متبقي</th>
          <th>زيادة</th>
          <th>أثر الرصيد</th>
        </tr>
      </thead>
      <tbody>
        {rows if rows else "<tr><td colspan='9' class='muted'>لا توجد حركات لعرضها.</td></tr>"}
      </tbody>
    </table>

    <hr/>
    <div class="muted" style="font-size:11px;">
      * (أثر الرصيد) محسوب من بيانات الفواتير: ذمم = +صافي، نقدي = +المتبقي - الزيادة كرصد.
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
# Main page
# ---------------------------
def orders_prep_page(go, user):
    st.markdown("<h2 style='text-align:center;'>🧑‍🍳 تحضير + تسليم الطلبات</h2>", unsafe_allow_html=True)
    st.caption("✅ التحضير يخصم المخزون فوراً — الدفع يتحدد عند التسليم — الرصيد: موجب=عليه، سالب=له رصيد")
    st.divider()

    # Back
    c_back, _, _ = st.columns([1, 2, 1])
    with c_back:
        if st.button("⬅️ رجوع", key="prep_back"):
            go("dashboard")

    # Session states
    st.session_state.setdefault("prep_cart", {})
    st.session_state.setdefault("last_print_sale_id", None)
    st.session_state.setdefault("last_print_customer_id", None)  # ✅ NEW
    st.session_state.setdefault("_print_mode", "invoice")  # invoice | receipt | statement
    st.session_state.setdefault("deliver_target_id", None)

    # ✅ Only one dialog can be open
    st.session_state.setdefault("active_dialog", None)  # None | "deliver" | "print"

    st.session_state.setdefault("cust_price_map", {})
    st.session_state.setdefault("cust_price_map_for", "")

    # Caches
    if "products_cache_prep" not in st.session_state:
        st.session_state.products_cache_prep = col_to_list("products", where_active=True)
    if "customers_cache_prep" not in st.session_state:
        st.session_state.customers_cache_prep = col_to_list("customers", where_active=True)

    r1, r2, _ = st.columns([1.2, 1.2, 1.6])
    with r1:
        if st.button("🔄 تحديث المنتجات", key="prep_refresh_products"):
            st.session_state.pop("products_cache_prep", None)
            st.rerun()
    with r2:
        if st.button("🔄 تحديث العملاء", key="prep_refresh_customers"):
            st.session_state.pop("customers_cache_prep", None)
            st.rerun()

    products = st.session_state.get("products_cache_prep", []) or []
    customers = st.session_state.get("customers_cache_prep", []) or []

    prod_by_id = {p["id"]: p for p in products}
    cust_by_id = {c["id"]: c for c in customers}
    cust_map = {c.get("name", c["id"]): c["id"] for c in customers}

    # ---------------------------
    # Deliver dialog
    # ---------------------------
    def _render_deliver_dialog_if_needed():
        sid = st.session_state.get("deliver_target_id")
        if not sid or st.session_state.get("active_dialog") != "deliver":
            return

        sale = doc_get("sales", sid) or {}
        sale["id"] = sid

        cust_id = sale.get("customer_id") or ""
        customer = doc_get("customers", cust_id) if cust_id else {}
        cur_bal = float(to_float((customer or {}).get("balance", 0)))
        net_show = float(to_float(sale.get("net", 0)))

        @st.dialog("✅ تسليم الطلب (تحديد الدفع) — ثم اطبع من قائمة المُسلّم")
        def _dlg():
            st.write(f"**فاتورة:** {sale.get('invoice_no') or sid}")
            st.write(f"**العميل:** {sale.get('customer_name') or '—'}")
            st.write(f"**الصافي:** {net_show:.2f}")

            if cur_bal > 0:
                st.warning(f"⚠️ على العميل ذمم: {cur_bal:.2f}")
            elif cur_bal < 0:
                st.success(f"✅ للعميل رصيد عندك: {abs(cur_bal):.2f}")
            else:
                st.info("✅ رصيد العميل صفر")

            pay = st.radio(
                "نوع الدفع عند التسليم",
                options=["cash", "credit"],
                format_func=lambda x: "دفع (نقدي)" if x == "cash" else "ذمم (آجل)",
                index=0,
                key="deliver_payment_pick",
            )

            paid_amount = 0.0
            extra_as_credit = True

            if pay == "cash":
                paid_amount = st.number_input(
                    "المبلغ المستلم من العميل",
                    min_value=0.0,
                    step=0.25,
                    value=float(net_show),
                    key="deliver_paid_amount",
                )
                extra_as_credit = st.checkbox(
                    "اعتبر الزيادة رصيد للعميل (لا تُرجع باقي)",
                    value=True,
                    key="deliver_extra_as_credit",
                )

                extra = max(0.0, float(paid_amount) - float(net_show))
                unpaid = max(0.0, float(net_show) - float(paid_amount))

                if extra > 0 and extra_as_credit:
                    st.success(f"✅ الزيادة ({extra:.2f}) ستُسجّل كرصيد للعميل (الرصيد قد يصبح سالب).")
                elif extra > 0 and not extra_as_credit:
                    st.info(f"ℹ️ الزيادة ({extra:.2f}) تعتبر باقي يُرجع للعميل (لا تؤثر على الرصيد).")

                if unpaid > 0:
                    st.warning(f"⚠️ دفع جزئي: المتبقي ذمم = {unpaid:.2f} (سيزيد رصيد العميل).")

            colA, colB = st.columns(2)
            with colA:
                if st.button("✅ تأكيد التسليم", use_container_width=True, key="deliver_confirm"):
                    try:
                        @firestore.transactional
                        def tx_deliver(transaction):
                            sale_ref = db.collection("sales").document(sid)
                            sale_snap = sale_ref.get(transaction=transaction)
                            if not sale_snap.exists:
                                raise ValueError("الفاتورة غير موجودة")

                            sd = sale_snap.to_dict() or {}

                            # لا تعيد التسليم
                            if sd.get("status") == "done":
                                return

                            # ✅ اقرأ كل شيء قبل أي كتابة
                            net_local = float(to_float(sd.get("net", 0)))
                            cust_id_local = sd.get("customer_id") or ""

                            # حسابات الدفع
                            paid = 0.0
                            extra = 0.0
                            unpaid = 0.0

                            if pay == "cash":
                                paid = float(to_float(paid_amount, 0.0))
                                extra = max(0.0, paid - net_local)
                                unpaid = max(0.0, net_local - paid)

                            # تحديد تغيير الرصيد (delta) مرة واحدة
                            balance_delta = 0.0
                            if pay == "credit":
                                balance_delta = +net_local
                            else:
                                # cash:
                                if unpaid > 0:
                                    balance_delta += unpaid  # يزيد دينه
                                if extra > 0 and extra_as_credit:
                                    balance_delta -= extra  # رصيد للعميل (قد يصير سالب)

                            # إذا سنغير الرصيد لازم نقرأ العميل (قبل أي كتابة)
                            cust_ref = None
                            cur_bal_local = 0.0

                            if abs(balance_delta) > 1e-12:
                                if not cust_id_local:
                                    raise ValueError("لا يوجد عميل مرتبط بالفاتورة (customer_id)")
                                cust_ref = db.collection("customers").document(cust_id_local)
                                cust_snap = cust_ref.get(transaction=transaction)
                                if not cust_snap.exists:
                                    raise ValueError("العميل غير موجود")
                                cust_data = cust_snap.to_dict() or {}
                                cur_bal_local = float(to_float(cust_data.get("balance", 0)))

                            # ✅ الآن اكتب
                            updates = {
                                "status": "done",
                                "payment_type": pay,  # cash | credit
                                "delivered_at": now_iso(),
                                "delivered_by": user.get("username", ""),
                                "updated_at": now_iso(),
                                # قيم الدفع (تظهر بالفاتورة/الإيصال)
                                "amount_paid": float(paid) if pay == "cash" else 0.0,
                                "extra_credit": float(extra) if (pay == "cash" and extra > 0 and extra_as_credit) else 0.0,
                                "unpaid_debt": float(unpaid) if (pay == "cash" and unpaid > 0) else 0.0,
                                "balance_applied": False,
                            }

                            # تحديث رصيد العميل (مرة واحدة)
                            if abs(balance_delta) > 1e-12:
                                new_bal = cur_bal_local + balance_delta
                                transaction.update(cust_ref, {"balance": new_bal, "updated_at": now_iso()})
                                updates["balance_applied"] = True

                            transaction.update(sale_ref, updates)

                        tx_deliver(db.transaction())

                        st.success("تم التسليم ✅ الآن اطبع الفاتورة/سند القبض من قائمة (المُسلّم).")
                        st.session_state.active_dialog = None
                        st.session_state.deliver_target_id = None
                        st.rerun()

                    except Exception as e:
                        st.error(f"فشل التسليم: {e}")

            with colB:
                if st.button("❌ إغلاق", use_container_width=True, key="deliver_close"):
                    st.session_state.active_dialog = None
                    st.session_state.deliver_target_id = None
                    st.rerun()

        _dlg()

    # ---------------------------
    # Print dialog (invoice / receipt / statement)
    # ---------------------------
    def _render_print_dialog_if_needed():
        if st.session_state.get("active_dialog") != "print":
            return

        mode = st.session_state.get("_print_mode", "invoice")

        @st.dialog("🖨️ طباعة")
        def _dlg():
            col1, col2 = st.columns([1, 1])
            with col1:
                paper = st.selectbox("نوع الورق", ["80mm", "a4"], index=0, key="print_paper_pick")
            with col2:
                if st.button("❌ إغلاق", use_container_width=True, key="print_close"):
                    st.session_state.active_dialog = None
                    st.session_state.last_print_sale_id = None
                    st.session_state.last_print_customer_id = None
                    st.rerun()

            if mode == "debt":
                cid = st.session_state.get("last_print_customer_id") or ""
                cust = doc_get("customers", cid) if cid else {}
                html = build_debt_only_invoice_html(
                    cust or {},
                    company_name="نظام المخبز",
                    paper=paper
                )
                show_print_html(html, height=820)
                return

            if mode == "statement":
                cid = st.session_state.get("last_print_customer_id") or ""
                cust = doc_get("customers", cid) if cid else {}
                sales = _get_customer_sales_for_statement(cid, limit=200) if cid else []
                html = build_customer_statement_html(
                    cust or {},
                    sales,
                    company_name="نظام المخبز",
                    paper=paper,
                    max_rows=30
                )
                show_print_html(html, height=820)
                return

            # invoice/receipt: تحتاج sale
            sid = st.session_state.get("last_print_sale_id")
            sale = doc_get("sales", sid) or {}
            sale["id"] = sid

            cust_id = sale.get("customer_id") or ""
            customer = doc_get("customers", cust_id) if cust_id else {}

            if mode == "receipt":
                html = build_receipt_html(sale, customer=customer or {}, company_name="نظام المخبز", paper=paper)
                show_print_html(html, height=820)
            else:
                html = build_invoice_html(sale, customer=customer or {}, company_name="نظام المخبز", paper=paper)
                show_print_html(html, height=820)

        _dlg()

    # ✅ Router: open ONLY ONE dialog per run
    if _supports_dialog():
        if st.session_state.get("active_dialog") == "deliver":
            _render_deliver_dialog_if_needed()
        elif st.session_state.get("active_dialog") == "print":
            _render_print_dialog_if_needed()

    # ---------------------------
    # New preparation
    # ---------------------------
    st.subheader("➕ تحضير طلب جديد (خصم مخزون فوراً)")

    if not customers:
        st.error("لا يوجد عملاء. أضف عملاء أولاً من صفحة العملاء.")
        return

    if not products:
        st.error("لا يوجد منتجات. أضف منتجات أولاً من صفحة المستودع.")
        return

    # ✅ radio لتحديد عميل/زائر (بدون حذف أي شيء)
    colT, colC, colD = st.columns([1.1, 2.2, 1.2])
    with colT:
        prep_kind = st.radio("النوع", ["عميل", "زائر"], horizontal=True, key="prep_kind")

    with colC:
        cust_name = st.selectbox(
            "اختر العميل",
            options=[""] + list(cust_map.keys()),
            key="prep_customer_select",
            disabled=(prep_kind == "زائر"),
        )

    with colD:
        discount = st.number_input("خصم (مبلغ)", min_value=0.0, step=0.25, value=0.0, key="prep_discount")

    # ✅ إذا عميل لازم يختار عميل، أما زائر لا
    if prep_kind == "عميل" and not cust_name:
        st.info("اختر عميل لبدء التحضير.")
        return

    # ✅ تحديد بيانات العميل حسب النوع
    if prep_kind == "عميل":
        customer_id = cust_map[cust_name]
        customer = cust_by_id.get(customer_id, {}) or {}

        # ✅ عرض رصيد العميل + زر طباعة كشف حساب (بدون التأثير على باقي المهام)
        cur_balance = float(to_float(customer.get("balance", 0)))

        b1, b2, b3 = st.columns([1.2, 1.1, 2.7])
        with b1:
            st.metric("رصيد العميل", f"{cur_balance:.2f}")
        with b2:
            if st.button("🖨️فاتورة ذمم", use_container_width=True, key="cust_statement_print_btn"):
                st.session_state.last_print_customer_id = customer_id
                st.session_state.last_print_sale_id = None
                st.session_state._print_mode = "debt"
                st.session_state.active_dialog = "print"
                st.rerun()
        with b3:
            if cur_balance > 0:
                st.warning(f"⚠️ على العميل ذمم: {cur_balance:.2f}")
            elif cur_balance < 0:
                st.success(f"✅ للعميل رصيد عندك: {abs(cur_balance):.2f}")
            else:
                st.info("✅ رصيد العميل صفر")

        # تحميل أسعار العميل الخاصة مرة واحدة (مخفي)
        if st.session_state.get("cust_price_map_for") != customer_id:
            st.session_state.cust_price_map = _get_customer_prices_map(customer_id)
            st.session_state.cust_price_map_for = customer_id

        cust_price_map = st.session_state.get("cust_price_map", {}) or {}

    else:
        # زائر: لا أسعار خاصة ولا رصيد
        customer_id = ""
        customer = {"name": "زائر"}
        cust_price_map = {}
        st.info("✅ وضع الزائر: لا يتم استخدام اسم عميل ولا رصيد ولا أسعار خاصة.")

    st.divider()
    st.markdown("### 🧱 المنتجات (مربعات)")

    # (اختياري) تحسين شكل الحدود
    st.markdown(
        """
        <style>
        div[data-testid="stVerticalBlockBorderWrapper"] {
          border-radius: 14px !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # ✅ Grid cards with free quantity button
    grid_cols = st.columns(4)
    for idx, p in enumerate(sorted(products, key=lambda x: (x.get("name") or ""))):
        pid = p["id"]
        name = p.get("name", pid)

        qty_in_cart = int(st.session_state.prep_cart.get(pid, 0))
        stock_int = int(to_int(to_float(p.get("qty_on_hand", 0)), 0))

        show_key = f"show_free_qty__{pid}"
        qty_key = f"free_qty__{pid}"
        st.session_state.setdefault(show_key, False)
        st.session_state.setdefault(qty_key, qty_in_cart)

        with grid_cols[idx % 4]:
            card = st.container(border=True)
            with card:
                st.markdown(f"**{name}**")
                st.caption(f"بالمخزن: {stock_int} | بالسلة: **{qty_in_cart}**")

                c1, c2, c3 = st.columns([1, 1, 1])
                with c1:
                    st.button(
                        "➕ +1",
                        use_container_width=True,
                        key=f"p_add1_{pid}",
                        on_click=_set_cart_qty,
                        args=(pid, qty_in_cart + 1, stock_int),
                    )
                with c2:
                    st.button(
                        "➕ +5",
                        use_container_width=True,
                        key=f"p_add5_{pid}",
                        on_click=_set_cart_qty,
                        args=(pid, qty_in_cart + 5, stock_int),
                    )
                with c3:
                    st.button(
                        "✏️ كمية",
                        use_container_width=True,
                        key=f"p_freebtn_{pid}",
                        on_click=_toggle_free_qty,
                        args=(pid,),
                    )

                if st.session_state.get(show_key, False):
                    st.number_input(
                        "أدخل الكمية",
                        min_value=0,
                        max_value=max(0, stock_int),
                        step=1,
                        key=qty_key,
                    )
                    a1, a2 = st.columns(2)
                    with a1:
                        st.button(
                            "✅ تطبيق",
                            use_container_width=True,
                            key=f"p_apply_{pid}",
                            on_click=_apply_free_qty,
                            args=(pid, stock_int),
                        )
                    with a2:
                        st.button(
                            "إخفاء",
                            use_container_width=True,
                            key=f"p_hide_{pid}",
                            on_click=_toggle_free_qty,
                            args=(pid,),
                        )

                m1, m2 = st.columns(2)
                with m1:
                    st.button(
                        "➖",
                        use_container_width=True,
                        key=f"p_minus_{pid}",
                        on_click=_set_cart_qty,
                        args=(pid, max(0, qty_in_cart - 1), stock_int),
                    )
                with m2:
                    st.button(
                        "0",
                        use_container_width=True,
                        key=f"p_zero_{pid}",
                        on_click=_set_cart_qty,
                        args=(pid, 0, stock_int),
                    )

    st.markdown("### 🧺 السلة")
    cart = st.session_state.prep_cart or {}
    if not cart:
        st.info("السلة فارغة.")
    else:
        items = []
        total = 0.0

        for pid, qty in cart.items():
            pr = prod_by_id.get(pid, {}) or {}
            pname = pr.get("name", pid)

            base_price = float(to_float(pr.get("price", 0)))
            used_price = float(cust_price_map.get(pid, base_price))  # مخفي عن الواجهة

            line_total = float(used_price) * float(qty)
            total += line_total

            items.append({
                "product_id": pid,
                "product_name": pname,
                "qty": int(qty),
                "price": float(used_price),  # محفوظ للفاتورة فقط
                "total": float(line_total),
            })

        net = float(total) - float(discount)

        st.dataframe(
            [{"الصنف": it["product_name"], "الكمية": it["qty"]} for it in items],
            use_container_width=True,
            hide_index=True
        )

        m1, m2, m3 = st.columns(3)
        m1.metric("الإجمالي", f"{total:.2f}")
        m2.metric("الخصم", f"{discount:.2f}")
        m3.metric("الصافي", f"{net:.2f}")

        colA, colB = st.columns(2)
        with colA:
            if st.button("💾 حفظ كطلب مُحضّر (خصم مخزون الآن)", use_container_width=True, key="prep_save"):
                inv = f"INV-{datetime.now(timezone(timedelta(hours=3))).strftime('%Y%m%d-%H%M%S')}"
                sale_id = inv.lower().replace(":", "").replace(" ", "_")

                @firestore.transactional
                def tx_prepare_and_deduct(transaction):
                    # قراءة المنتجات والتحقق
                    prod_refs = []
                    snaps = []
                    for it in items:
                        ref = db.collection("products").document(it["product_id"])
                        snap = ref.get(transaction=transaction)
                        if not snap.exists:
                            raise ValueError(f"منتج غير موجود: {it.get('product_name','')}")
                        prod_refs.append(ref)
                        snaps.append(snap)

                    # تحقق المخزون
                    for it, snap in zip(items, snaps):
                        cur = float(to_float((snap.to_dict() or {}).get("qty_on_hand", 0)))
                        req = float(it["qty"])
                        if cur < req:
                            raise ValueError(
                                f"المخزون غير كافي للمنتج: {it['product_name']} (المطلوب {req}, المتوفر {cur})"
                            )

                    # خصم المخزون
                    for it, ref, snap in zip(items, prod_refs, snaps):
                        cur = float(to_float((snap.to_dict() or {}).get("qty_on_hand", 0)))
                        req = float(it["qty"])
                        transaction.update(ref, {"qty_on_hand": cur - req, "updated_at": now_iso()})

                    # حفظ الفاتورة prepared
                    sale_ref = db.collection("sales").document(sale_id)
                    transaction.set(sale_ref, {
                        "invoice_no": inv,
                        "ref": inv,
                        "customer_id": (customer_id or ""),
                        "customer_name": (customer.get("name", "") if prep_kind == "عميل" else "زائر"),
                        "payment_type": None,            # يتحدد عند التسليم
                        "discount": float(discount),
                        "total": float(total),
                        "net": float(net),
                        "items": items,
                        "status": "prepared",
                        "stock_deducted": True,
                        "balance_applied": False,
                        "amount_paid": 0.0,
                        "extra_credit": 0.0,
                        "unpaid_debt": 0.0,
                        "active": True,
                        "created_at": now_iso(),
                        "updated_at": now_iso(),
                        "created_by": user.get("username", ""),
                    }, merge=True)

                try:
                    tx_prepare_and_deduct(db.transaction())

                    # حركة مخزون (خصم أثناء التحضير)
                    for it in items:
                        write_stock_move({
                            "type": "sale",
                            "ref_type": "sale_prepared",
                            "ref_id": sale_id,
                            "item_type": "product",
                            "item_id": it["product_id"],
                            "item_name": it.get("product_name", ""),
                            "qty_delta": -float(it["qty"]),
                            "unit": (prod_by_id.get(it["product_id"], {}) or {}).get("sale_unit", "pcs"),
                            "note": "خصم أثناء تحضير الطلب (قبل التسليم)",
                            "created_by": user.get("username", ""),
                        })

                    _clear_prep_cart_and_free_qty_keys()
                    st.success("تم حفظ الطلب كمُحضّر ✅ وتم خصم المخزون ✅")
                    st.rerun()
                except Exception as e:
                    st.error(f"فشل التحضير/الخصم: {e}")

        with colB:
            if st.button("🧹 تفريغ السلة", use_container_width=True, key="prep_clear"):
                _clear_prep_cart_and_free_qty_keys()
                st.rerun()

    # ---------------------------
    # Lists: Prepared + Done
    # ---------------------------
    st.divider()
    st.subheader("📦 طلبات مُحضّرة جاهزة للتسليم")

    docs = db.collection("sales").limit(300).stream()
    prepared = []
    done = []

    for d in docs:
        x = d.to_dict() or {}
        if x.get("active") is not True:
            continue
        x["id"] = d.id

        if x.get("status") == "prepared":
            prepared.append(x)
        elif x.get("status") == "done":
            done.append(x)

    prepared.sort(key=lambda x: (x.get("created_at") or ""), reverse=True)
    done.sort(key=lambda x: (x.get("delivered_at") or x.get("updated_at") or ""), reverse=True)

    # Prepared list (تسليم فقط)
    if not prepared:
        st.info("لا يوجد طلبات مُحضّرة حالياً.")
    else:
        for o in prepared[:80]:
            sid = o["id"]
            inv = o.get("invoice_no") or o.get("ref") or sid
            cname = o.get("customer_name") or "—"
            net_v = float(to_float(o.get("net", 0)))

            row1, row2 = st.columns([4.6, 1.4])
            with row1:
                st.markdown(f"**{inv}** — {cname} | الصافي: **{net_v:.2f}**")
            with row2:
                if st.button("✅ تسليم", use_container_width=True, key=f"deliver_{sid}"):
                    if _supports_dialog():
                        st.session_state.deliver_target_id = sid
                        st.session_state.last_print_sale_id = None
                        st.session_state.last_print_customer_id = None
                        st.session_state.active_dialog = "deliver"
                        st.rerun()
                    else:
                        st.error("نسخة Streamlit لا تدعم Dialog. حدّث Streamlit أو اطلب مني نسخة بدون Dialog.")
            st.divider()

    # Done list: print invoice + receipt
    # Done list: print invoice + receipt
    st.divider()
    st.subheader("✅ فواتير مُسلّمة (اطبع من هنا)")

    if not done:
        st.info("لا يوجد فواتير مُسلّمة حالياً.")
        return

    # ✅ Pagination: show 20, load 20 more
    st.session_state.setdefault("done_show_n", 20)

    c_reset, c_more = st.columns([1, 1])
    with c_reset:
        if st.button("↩️ إعادة ضبط العرض", use_container_width=True, key="done_reset"):
            st.session_state.done_show_n = 20
            st.rerun()

    with c_more:
        can_more = st.session_state.done_show_n < len(done)
        if st.button("➕ عرض باقي الفواتير", use_container_width=True, key="done_more", disabled=not can_more):
            st.session_state.done_show_n += 20
            st.rerun()

    show_n = min(int(st.session_state.done_show_n), len(done))
    st.caption(f"عرض {show_n} من أصل {len(done)} فاتورة مُسلّمة")

    for o in done[:show_n]:
        sid = o["id"]
        inv = o.get("invoice_no") or o.get("ref") or sid
        cname = o.get("customer_name") or "—"
        net_v = float(to_float(o.get("net", 0)))
        ptype = o.get("payment_type")  # cash | credit
        paid = float(to_float(o.get("amount_paid", 0)))

        left, b1, b2 = st.columns([4.2, 0.9, 0.9])
        with left:
            pay_txt = "ذمم" if ptype == "credit" else ("نقدي" if ptype == "cash" else "غير محدد")
            st.markdown(f"**{inv}** — {cname} | الصافي: **{net_v:.2f}** | الدفع: **{pay_txt}**")

        with b1:
            if st.button("🖨️ فاتورة", use_container_width=True, key=f"done_print_invoice_{sid}"):
                st.session_state.last_print_sale_id = sid
                st.session_state.last_print_customer_id = None
                st.session_state._print_mode = "invoice"
                st.session_state.active_dialog = "print"
                st.rerun()

        with b2:
            can_receipt = (ptype == "cash") and (paid > 0)
            if st.button("🧾 قبض", use_container_width=True, key=f"done_print_receipt_{sid}", disabled=not can_receipt):
                st.session_state.last_print_sale_id = sid
                st.session_state.last_print_customer_id = None
                st.session_state._print_mode = "receipt"
                st.session_state.active_dialog = "print"
                st.rerun()

        st.divider()
