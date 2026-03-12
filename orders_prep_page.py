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

 

def _supports_dialog():
    return hasattr(st, "dialog")

def write_stock_moves_batch(moves: list[dict]):
    if not moves:
        return

    batch = db.batch()
    ts = now_iso()

    for move in moves:
        move["created_at"] = ts
        move["active"] = True
        ref = db.collection("stock_moves").document()
        batch.set(ref, move)

    batch.commit()
    
@st.cache_data(ttl=120)
def get_distributor_name(dist_id: str) -> str:
    if not dist_id:
        return ""
    snap = db.collection("distributors").document(dist_id).get()
    if snap.exists:
        x = snap.to_dict() or {}
        return (x.get("name") or "").strip()
    return ""
# ---------------------------
# CACHED LOADERS (🔥 أسرع بكثير)
# ---------------------------
@st.cache_data(ttl=60)
def load_products_cached():
    return col_to_list("products", where_active=True)

@st.cache_data(ttl=60)
def load_customers_cached():
    return col_to_list("customers", where_active=True)

# ---------------------------
# UI helpers (Free qty + Card)
# ---------------------------
def _apply_free_qty(pid: str, stock_int: int):
    qty_key = f"free_qty__{pid}"
    raw = st.session_state.get(qty_key, None)
    q = int(to_int(raw, 0))

    if q < 0:
        q = 0
    if q > int(stock_int):
        q = int(stock_int)

    if q == 0:
        st.session_state.prep_cart.pop(pid, None)
    else:
        st.session_state.prep_cart[pid] = q


def _clear_prep_cart_and_free_qty_keys():
    st.session_state.prep_cart = {}
    for k in list(st.session_state.keys()):
        if k.startswith("free_qty__") or k.startswith("show_free_qty__"):
            st.session_state.pop(k, None)


# ---------------------------
# Customer special prices (hidden)
# ---------------------------
@st.cache_data(ttl=60)
def _get_customer_prices_map_cached(customer_id: str, limit=400):
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
    seller_username = sale.get("seller_username", "")

    total = to_float(sale.get("total", 0))
    discount = to_float(sale.get("discount", 0))
    net = float(total) - float(discount)

    ptype = sale.get("payment_type")

    paid = float(to_float(sale.get("amount_paid", 0)))
    old_debt_paid = float(to_float(sale.get("old_debt_paid", 0)))
    old_debt_remaining = float(to_float(sale.get("old_debt_remaining", 0)))
    final_due = float(to_float(sale.get("final_due", 0)))

    extra_credit = float(to_float(sale.get("extra_credit", 0)))
    unpaid = float(to_float(sale.get("unpaid_debt", 0)))

    total_collected = paid + old_debt_paid

    if ptype == "credit":
        header_type = "فاتورة ذمم - آجل"
    elif ptype == "cash":
        if unpaid > 0:
            header_type = "فاتورة دفع جزئي + ذمم متبقي"
        else:
            header_type = "فاتورة نقدي - مدفوعة"
    else:
        header_type = "فاتورة"

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

    pay_lines = ""

    if ptype == "cash":
        pay_lines += f"<div class='sumrow'><span>المسدّد لهذه الفاتورة:</span><span><b>{_money(paid)}</b></span></div>"

        if extra_credit > 0:
            pay_lines += f"<div class='sumrow'><span>زيادة كرصد للعميل:</span><span><b>{_money(extra_credit)}</b></span></div>"

        if unpaid > 0:
            pay_lines += f"<div class='sumrow'><span>متبقي من هذه الفاتورة:</span><span><b>{_money(unpaid)}</b></span></div>"

    if old_debt_paid > 0:
        pay_lines += f"<div class='sumrow'><span>ذمم سابقة مسددة:</span><span><b>{_money(old_debt_paid)}</b></span></div>"

    if total_collected > 0:
        pay_lines += f"<div class='sumrow' style='font-size:16px;'><span>إجمالي المقبوض:</span><span><b>{_money(total_collected)}</b></span></div>"
    
    if final_due > 0:
        pay_lines += f"<div class='sumrow'><span>إجمالي الذمم المستحقة:</span><span><b>{_money(final_due)}</b></span></div>"

    if paper == "a4":
        page_css = """        
        body{font-family:Arial;direction:rtl;font-weight:bold;}
        .wrap{width:190mm;margin:auto;padding:10mm}
        table{width:100%;border-collapse:collapse}
        th,td{padding:6px;border-bottom:1px solid #ddd;text-align:right}
        .sumrow{display:flex;justify-content:space-between;margin-top:6px}
        .center{text-align:center}
        .badge{border:1px solid #ccc;border-radius:20px;padding:4px 10px;display:inline-block;margin-top:6px}
        .btnbar{margin-top:10px;display:flex}
        button{width:100%;padding:10px;font-size:14px;font-weight:bold;cursor:pointer}
        @media print{
          .btnbar{display:none}
        }
        """
    else:
         page_css = """
        body{font-family:Arial;direction:rtl;font-weight:bold;width:58mm;margin:0}
        .wrap{width:54mm;margin:auto;padding:2mm}
        table{width:100%;border-collapse:collapse}
        th,td{padding:3px;border-bottom:1px solid #ddd;text-align:right;font-size:11px}
        .sumrow{display:flex;justify-content:space-between;margin-top:4px}
        .center{text-align:center}
        .badge{border:1px solid #ccc;border-radius:20px;padding:4px 8px;display:inline-block;margin-top:6px}
        .btnbar{margin-top:10px;display:flex}
        button{width:100%;padding:10px;font-size:14px;font-weight:bold;cursor:pointer}
        @media print{
          .btnbar{display:none}
        }
        """

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<style>
{page_css}
</style>
</head>

<body>
<div class="wrap">

<div class="center">
<div style="font-size:18px">{company_name}</div>
<div class="badge">{header_type}</div>
</div>

<hr>

<div class="sumrow"><span>رقم الفاتورة:</span><span>{invoice_no}</span></div>
<div class="sumrow"><span>التاريخ:</span><span>{dt}</span></div>
<div class="sumrow"><span>العميل:</span><span>{cust_name}</span></div>
<div class="sumrow"><span>الموزع:</span><span>{seller_username}</span></div>

{"<div class='sumrow'><span>هاتف:</span><span>"+phone+"</span></div>" if phone else ""}

<hr>

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

<hr>

<div class="sumrow"><span>الإجمالي:</span><span>{_money(total)}</span></div>
<div class="sumrow"><span>خصم:</span><span>{_money(discount)}</span></div>
<div class="sumrow" style="font-size:16px"><span>الصافي:</span><span>{_money(net)}</span></div>

{pay_lines}

<hr>
<div class="center">شكراً لزيارتكم ❤️</div>

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
    old_debt_paid = float(to_float(sale.get("old_debt_paid", 0)))
    old_debt_remaining = float(to_float(sale.get("old_debt_remaining", 0)))
    final_due = float(to_float(sale.get("final_due", 0)))
    extra_credit = float(to_float(sale.get("extra_credit", 0)))
    unpaid = float(to_float(sale.get("unpaid_debt", 0)))

    total_collected = paid + old_debt_paid

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
    <div class="sumrow"><span>المسدّد لهذه الفاتورة:</span><span><b>{_money(paid)}</b></span></div>

    {f"<div class='sumrow'><span>ذمم سابقة مسددة:</span><span><b>{_money(old_debt_paid)}</b></span></div>" if old_debt_paid > 0 else ""}
    {f"<div class='sumrow'><span>المتبقي من الذمم السابقة:</span><span><b>{_money(old_debt_remaining)}</b></span></div>" if old_debt_remaining > 0 else ""}
    {f"<div class='sumrow'><span>إجمالي الذمم بعد الفاتورة:</span><span><b>{_money(final_due)}</b></span></div>" if final_due > 0 else ""}

{f"<div class='sumrow'><span>إجمالي المقبوض:</span><span><b>{_money(total_collected)}</b></span></div>" if total_collected > 0 else ""}
    {f"<div class='sumrow'><span>زيادة كرصد للعميل:</span><span><b>{_money(extra_credit)}</b></span></div>" if extra_credit > 0 else ""}
    {f"<div class='sumrow'><span>متبقي من هذه الفاتورة:</span><span><b>{_money(unpaid)}</b></span></div>" if unpaid > 0 else ""}
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

@st.cache_data(ttl=30)
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

def show_print_html(html, height=1100):
    components.html(html, height=height, scrolling=False)

#------
# تسديد ذمم
#-------
def build_debt_payment_receipt_html(customer: dict, amount: float, company_name="نظام المخبز", paper="80mm"):
    customer = customer or {}
    cust_name = customer.get("name") or "—"
    phone = customer.get("phone") or ""
    dt = datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")

    width_css = "800px" if paper == "a4" else "280px"
    font_css = "14px" if paper == "a4" else "12px"

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Debt Payment Receipt</title>
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
      <div class="badge">سند قبض تسديد ذمم</div>
    </div>

    <hr/>

    <div class="sumrow"><span>التاريخ:</span><span>{dt}</span></div>
    <div class="sumrow"><span>العميل:</span><span><b>{cust_name}</b></span></div>
    {f"<div class='sumrow'><span>هاتف:</span><span>{phone}</span></div>" if phone else ""}

    <hr/>

    <div class="sumrow"><span>المبلغ المقبوض:</span><span><b>{_money(amount)}</b></span></div>

    <hr/>
    <div class="center muted">هذا السند يثبت تسديد ذمم للعميل.</div>

    <div class="btnbar">
      <button onclick="window.print()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html
#------
# تسديد ذمم
#-------


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
    st.session_state.setdefault("last_print_customer_id", None)
    st.session_state.setdefault("_print_mode", "invoice")
    st.session_state.setdefault("deliver_target_id", None)
    st.session_state.setdefault("active_dialog", None)
    st.session_state.setdefault("cust_price_map", {})
    st.session_state.setdefault("cust_price_map_for", "")
    st.session_state.setdefault("last_debt_payment_amount", 0.0)

    # ---------------------------------------------------------------
    # Caches
    #----------------------------------------------------------------



    r1, r2, _ = st.columns([1.2, 1.2, 1.6])

    with r1:
        if st.button("🔄 تحديث المنتجات", key="prep_refresh_products"):
            load_products_cached.clear()
            st.rerun()

    with r2:
        if st.button("🔄 تحديث العملاء", key="prep_refresh_customers"):
            load_customers_cached.clear()
            _get_customer_prices_map_cached.clear()
            st.rerun()

    products = load_products_cached()
    customers = load_customers_cached()
    
    prod_by_id = {p["id"]: p for p in products}
    cust_by_id = {c["id"]: c for c in customers}
    cust_map = {c.get("name", c["id"]): c["id"] for c in customers}

    # ---------------------------
    # Deliver dialog
    # ---------------------------
    def _render_debt_payment_dialog_if_needed():
        cid = st.session_state.get("last_print_customer_id")
        if not cid or st.session_state.get("active_dialog") != "debt_payment":
            return

        customer = doc_get("customers", cid) or {}
        customer["id"] = cid
        cur_bal = float(to_float(customer.get("balance", 0)))

        @st.dialog("💰 تسديد ذمم العميل")
        def _dlg():
            st.write(f"**العميل:** {customer.get('name') or '—'}")

            if cur_bal <= 0:
                st.info("لا يوجد ذمم مستحقة على هذا العميل.")
                if st.button("إغلاق", use_container_width=True, key=f"close_debt_payment_{cid}"):
                    st.session_state.active_dialog = None
                    st.rerun()
                return

            st.warning(f"⚠️ الذمم الحالية على العميل: {cur_bal:.3f}")

            pay_key = f"debt_payment_amount_{cid}"
            if pay_key not in st.session_state:
                st.session_state[pay_key] = None

            amount = st.number_input(
                "مبلغ التسديد",
                min_value=0.0,
                max_value=float(cur_bal),
                step=0.25,
                value=st.session_state[pay_key],
                key=f"debt_payment_input_{cid}",
                placeholder="أدخل مبلغ التسديد",
            )

            st.session_state[pay_key] = amount
            amount = float(to_float(amount, 0.0))

            colA, colB = st.columns(2)

            with colA:
                if st.button("✅ حفظ وطباعة السند", use_container_width=True, key=f"save_debt_payment_{cid}"):
                    try:
                        if amount <= 0:
                            raise ValueError("أدخل مبلغ تسديد أكبر من صفر")

                        @firestore.transactional
                        def tx_pay_debt(transaction):
                            ts = now_iso()
                            cust_ref = db.collection("customers").document(cid)
                            cust_snap = cust_ref.get(transaction=transaction)

                            if not cust_snap.exists:
                                raise ValueError("العميل غير موجود")

                            cust_data = cust_snap.to_dict() or {}
                            bal = float(to_float(cust_data.get("balance", 0)))

                            if bal <= 0:
                                raise ValueError("لا يوجد ذمم مستحقة على هذا العميل")

                            if amount > bal:
                                raise ValueError("مبلغ التسديد أكبر من الذمم المستحقة")

                            new_bal = bal - amount

                            transaction.update(cust_ref, {
                                "balance": new_bal,
                                "updated_at": ts,
                            })

                        tx_pay_debt(db.transaction())

                        db.collection("customer_balance_moves").add({
                            "customer_id": cid,
                            "customer_name": customer.get("name") or "",
                            "sale_id": "",
                            "invoice_no": "",
                            "type": "debt_payment_only",
                            "amount": float(amount),
                            "active": True,
                            "created_at": now_iso(),
                            "created_by": user.get("username", ""),
                            "note": "تسديد ذمم من شاشة تحضير الطلبات"
                        })

                        load_customers_cached.clear()
                        _get_customer_prices_map_cached.clear()
                        _get_customer_sales_for_statement.clear()

                        st.session_state.last_debt_payment_amount = float(amount)
                        st.session_state._print_mode = "debt_payment_only"
                        st.session_state.active_dialog = "print"

                        st.session_state.pop(pay_key, None)
                        st.session_state.pop(f"debt_payment_input_{cid}", None)

                        st.rerun()

                    except Exception as e:
                        st.error(f"فشل تسديد الذمم: {e}")

            with colB:
                if st.button("❌ إغلاق", use_container_width=True, key=f"cancel_debt_payment_{cid}"):
                    st.session_state.pop(pay_key, None)
                    st.session_state.pop(f"debt_payment_input_{cid}", None)
                    st.session_state.active_dialog = None
                    st.rerun()

        _dlg()


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

            # =====================================================
            # نوع الدفع
            # =====================================================

            pay = st.radio(
                "نوع الدفع عند التسليم",
                options=["cash", "credit"],
                format_func=lambda x: "دفع (نقدي)" if x == "cash" else "ذمم (آجل)",
                index=0,
                key="deliver_payment_pick",
            )
            
            # =====================================================
            # 🔥 عرض الرصيد + زر إضافة الذمم (يعمل فقط عند الدفع نقدي)
            # =====================================================

          

            # ✅ حقل مستقل لتسديد الذمم السابقة
            # old_debt_key = f"deliver_old_debt_paid_{sid}"
            old_debt_state_key = f"deliver_old_debt_paid_state_{sid}"
            old_debt_widget_key = f"deliver_old_debt_paid_input_{sid}"

            # ✅ القيمة الداخلية تبدأ فارغة
            if old_debt_state_key not in st.session_state:
      
                st.session_state[old_debt_state_key] = None

            if cur_bal > 0:
                col_debt, col_plus = st.columns([4, 1])

                with col_debt:
                    st.warning(f"⚠️ على العميل ذمم سابقة: {cur_bal:.2f}")

                with col_plus:
                    if pay == "cash":
                        if st.button("➕", key=f"fill_old_debt_paid_{sid}", use_container_width=True):
                            st.session_state[old_debt_state_key] = float(cur_bal)
                            st.rerun()
                    else:
                        st.empty()

            elif cur_bal < 0:
                st.success(f"✅ للعميل رصيد عندك: {abs(cur_bal):.2f}")
            else:
                st.info("✅ رصيد العميل صفر")

            old_debt_paid = st.number_input(
                "تسديد ذمم سابقة",
                min_value=0.0,
                max_value=max(0.0, float(cur_bal)) if cur_bal > 0 else 0.0,
                step=0.25,
                value=st.session_state[old_debt_state_key],
                key=old_debt_widget_key,
                placeholder="أدخل قيمة التسديد",
                help="أدخل هنا المبلغ الذي تم تسديده من الذمم السابقة، أو اضغط زر + لتعبئة كامل الذمم المستحقة.",
                disabled=(cur_bal <= 0),
            )

            # ✅ خزّن القيمة بعد القراءة
            st.session_state[old_debt_state_key] = old_debt_paid
            old_debt_paid = float(to_float(old_debt_paid, 0.0))


            # =====================================================
            # ضبط القيمة الافتراضية مرة واحدة فقط
            # =====================================================

            # =====================================================
            # مبلغ الفاتورة الحالية
            # =====================================================

            # المبلغ المستلم = صافي الفاتورة
            paid_amount = float(net_show)

            st.number_input(
                "المبلغ المستلم لهذه الفاتورة",
                min_value=0.0,
                value=float(net_show),
                format="%.3f",
                key=f"deliver_paid_amount_view_{sid}",
                disabled=True
            )


            if pay != "cash":
                st.info("الفاتورة الحالية ذمم — يمكنك فقط تسجيل تسديد ذمم سابقة من الحقل أعلاه.")
            # =====================================================
            # أزرار التحكم
            # =====================================================

            colA, colB = st.columns(2)

            with colA:
                if st.button("✅ تأكيد التسليم", use_container_width=True):

                    try:

                        @firestore.transactional
                        def tx_deliver(transaction):
                            ts = now_iso()    
                            sale_ref = db.collection("sales").document(sid)
                            sale_snap = sale_ref.get(transaction=transaction)
                            if not sale_snap.exists:
                                raise ValueError("الفاتورة غير موجودة")

                            sd = sale_snap.to_dict() or {}
                            if sd.get("status") == "done":
                                return

                            net_local = float(to_float(sd.get("net", 0)))
                            cust_id_local = sd.get("customer_id") or ""

                            paid = 0.0
                            extra = 0.0
                            unpaid = 0.0
                            old_debt_paid_local = float(to_float(old_debt_paid, 0.0))

                            cust_ref = None
                            cur_bal_local = 0.0

                            if cust_id_local:
                                cust_ref = db.collection("customers").document(cust_id_local)
                                cust_snap = cust_ref.get(transaction=transaction)

                                if not cust_snap.exists:
                                    raise ValueError("العميل غير موجود")

                                cust_data = cust_snap.to_dict() or {}
                                cur_bal_local = float(to_float(cust_data.get("balance", 0)))

                            if old_debt_paid_local < 0:
                                raise ValueError("مبلغ تسديد الذمم السابقة غير صالح")

                            if old_debt_paid_local > max(0.0, cur_bal_local):
                                raise ValueError("مبلغ تسديد الذمم السابقة أكبر من الذمم المستحقة على العميل")

                            if pay == "cash":
                                paid = float(to_float(paid_amount, 0.0))
                                extra = max(0.0, paid - net_local)
                                unpaid = max(0.0, net_local - paid)

                            invoice_effect = 0.0

                            if pay == "credit":
                                invoice_effect = +net_local
                            else:
                                if unpaid > 0:
                                    invoice_effect += unpaid
                                if extra > 0:
                                    invoice_effect -= extra

                            balance_delta = invoice_effect - old_debt_paid_local

                            old_debt_remaining_local = max(0.0, cur_bal_local - old_debt_paid_local)
                            final_due_local = max(0.0, cur_bal_local + balance_delta)
                            updates = {
                                "status": "done",
                                "payment_type": pay,
                                "delivered_at": ts,
                                "delivered_by": user.get("username", ""),
                                "updated_at": ts,
                                "amount_paid": float(paid) if pay == "cash" else 0.0,
                                "extra_credit": float(extra) if pay == "cash" and extra > 0 else 0.0,
                                "unpaid_debt": float(unpaid) if pay == "cash" and unpaid > 0 else 0.0,
                                "old_debt_paid": float(old_debt_paid_local) if old_debt_paid_local > 0 else 0.0,
                                "old_debt_remaining": float(old_debt_remaining_local),
                                "final_due": float(final_due_local),
                                "balance_applied": False,
                                "distributor_id": (sd.get("distributor_id") or sd.get("seller_username") or user.get("username","")),
                                "distributor_name": (
                                    sd.get("distributor_name")
                                    or get_distributor_name(
                                        sd.get("distributor_id")
                                        or sd.get("seller_username")
                                        or user.get("username", "")
                                    )
                                ),
                            }

                            if abs(balance_delta) > 1e-12:
                                new_bal = cur_bal_local + balance_delta
                                transaction.update(cust_ref, {
                                     "balance": new_bal,
                                     "updated_at": ts
                                })

                                updates["balance_applied"] = True

                            transaction.update(sale_ref, updates)

                        tx_deliver(db.transaction())
                        load_customers_cached.clear()
                        _get_customer_prices_map_cached.clear()
                        _get_customer_sales_for_statement.clear()
                        old_paid_after = float(to_float(st.session_state.get(f"deliver_old_debt_paid_state_{sid}", 0.0), 0.0))
                        if old_paid_after > 0:
                            db.collection("customer_balance_moves").add({
                                "customer_id": sale.get("customer_id") or "",
                                "customer_name": sale.get("customer_name") or "",
                                "sale_id": sid,
                                "invoice_no": sale.get("invoice_no") or sid,
                                "type": "debt_payment",
                                "amount": float(old_paid_after),
                                "active": True,
                                "created_at": now_iso(),
                                "created_by": user.get("username", ""),
                                "note": "تسديد ذمم سابقة أثناء تسليم فاتورة"
                            })
                        # 🔥 تنظيف Session State
                        st.session_state.pop(f"deliver_paid_amount_{sid}", None)
                        st.session_state.pop(f"deliver_old_debt_paid_state_{sid}", None)
                        st.session_state.pop(f"deliver_old_debt_paid_input_{sid}", None)
 
                        st.success("تم التسليم ✅")

                        st.session_state.last_print_sale_id = sid
                        st.session_state.last_print_customer_id = None

                        # 🔥 دائماً فاتورة (وليس سند قبض)
                        st.session_state._print_mode = "invoice"

                        st.session_state.active_dialog = "print"
                        st.session_state.deliver_target_id = None

                        st.rerun()

                    except Exception as e:
                        st.error(f"فشل التسليم: {e}")

            with colB:
                if st.button("❌ إغلاق", use_container_width=True):
                    st.session_state.pop(f"deliver_paid_amount_{sid}", None)
                    st.session_state.pop(f"deliver_old_debt_paid_state_{sid}", None)
                    st.session_state.pop(f"deliver_old_debt_paid_input_{sid}", None)

                    st.session_state.active_dialog = None
                    st.session_state.deliver_target_id = None
                    st.rerun()
        _dlg()
       
    # ---------------------------
    # Print dialog
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
                html = build_debt_only_invoice_html(cust or {}, company_name="نظام المخبز", paper=paper)
                show_print_html(html, height=820)
                return

            if mode == "statement":
                cid = st.session_state.get("last_print_customer_id") or ""
                cust = doc_get("customers", cid) if cid else {}
                sales = _get_customer_sales_for_statement(cid, limit=200) if cid else []
                html = build_customer_statement_html(cust or {}, sales, company_name="نظام المخبز", paper=paper, max_rows=30)
                show_print_html(html, height=820)
                return
            #--للتسديد
            if mode == "debt_payment_only":
                cid = st.session_state.get("last_print_customer_id") or ""
                cust = doc_get("customers", cid) if cid else {}
                amount = float(to_float(st.session_state.get("last_debt_payment_amount", 0.0), 0.0))
                html = build_debt_payment_receipt_html(cust or {}, amount=amount, company_name="نظام المخبز", paper=paper)
                show_print_html(html, height=820)
                return
            
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
        elif st.session_state.get("active_dialog") == "debt_payment":
            _render_debt_payment_dialog_if_needed()
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

    colT, colC, colD = st.columns([1.1, 2.2, 1.2])

    is_distributor = (user.get("role") == "distributor")
    prep_kind_options = ["عميل"] if is_distributor else ["عميل", "زائر"]

    if is_distributor:
        st.session_state["prep_kind"] = "عميل"

    with colT:
        prep_kind = st.radio(
            "النوع",
            prep_kind_options,
            horizontal=True,
            key="prep_kind"
        )

    with colC:
        cust_name = st.selectbox(
            "اختر العميل",
            options=[""] + list(cust_map.keys()),
            key="prep_customer_select",
            disabled=(prep_kind == "زائر"),
        )

    with colD:
        discount = st.number_input(
            "خصم (مبلغ)",
            min_value=0.0,
            step=0.25,
            value=None,
            placeholder="أدخل الخصم",
            key="prep_discount"
        )
        discount = float(to_float(discount, 0.0))
        
    if prep_kind == "عميل" and not cust_name:
        st.info("اختر عميل لبدء التحضير.")
        return

    if prep_kind == "عميل":
        customer_id = cust_map[cust_name]
        customer = cust_by_id.get(customer_id, {}) or {}

        cur_balance = float(to_float(customer.get("balance", 0)))

        b1, b2, b3 = st.columns([1.2, 1.1, 2.7])
        with b1:
            st.metric("رصيد العميل", f"{cur_balance:.2f}")
        with b2:
            if st.button("💰 تسديد", use_container_width=True, key="cust_statement_print_btn", disabled=(cur_balance <= 0)):
                st.session_state.last_print_customer_id = customer_id
                st.session_state.last_print_sale_id = None
                st.session_state.last_debt_payment_amount = 0.0
                st.session_state.active_dialog = "debt_payment"
                st.rerun()
        with b3:
            if cur_balance > 0:
                st.warning(f"⚠️ على العميل ذمم: {cur_balance:.2f}")
            elif cur_balance < 0:
                st.success(f"✅ للعميل رصيد عندك: {abs(cur_balance):.2f}")
            else:
                st.info("✅ رصيد العميل صفر")

        if st.session_state.get("cust_price_map_for") != customer_id:
            st.session_state.cust_price_map = _get_customer_prices_map_cached(customer_id)
            st.session_state.cust_price_map_for = customer_id

        cust_price_map = st.session_state.get("cust_price_map", {}) or {}

    else:
        customer_id = ""
        customer = {"name": "زائر"}
        cust_price_map = {}
        st.info("✅ وضع الزائر: لا يتم استخدام اسم عميل ولا رصيد ولا أسعار خاصة.")

    st.divider()
    st.markdown("""
<style>
div[data-testid="stForm"] button {
    background-color: #22c55e !important;
    color: white !important;
    border: 1px solid #22c55e !important;
    font-weight: 700 !important;
}

div[data-testid="stForm"] button:hover {
    background-color: #16a34a !important;
    border-color: #15803d !important;
    color: white !important;
}
</style>
""", unsafe_allow_html=True)
    with st.form("prep_products_form", clear_on_submit=False):
        # ✅ مثل صفحة الموزعين: اختيار الأصناف على شكل tags
        default_names = []
        for pid in list((st.session_state.prep_cart or {}).keys()):
            if pid in prod_by_id:
                default_names.append(prod_by_id[pid].get("name", pid))

        all_products = [{"id": p["id"], "name": p.get("name", p["id"])} for p in products]
        all_products.sort(key=lambda r: (r["name"] or ""))

        chosen = st.multiselect(
            "اختر الأصناف",
            options=[p["name"] for p in all_products],
            default=default_names[:20],
            key="prep_load_choose",
            placeholder="اختر الأصناف من هنا"
        )

        name_to_id = {p["name"]: p["id"] for p in all_products}

         
        for nm in chosen:
            pid = name_to_id[nm]
            prod = prod_by_id.get(pid, {}) or {}
            stock_int = int(to_int(to_float(prod.get("qty_on_hand", 0)), 0))
            qty_in_cart = int(st.session_state.prep_cart.get(pid, 0))

            qty_key = f"free_qty__{pid}"

            if qty_key not in st.session_state:
                st.session_state[qty_key] = (qty_in_cart if qty_in_cart > 0 else None)

            cols = st.columns([3.8, 1.0], gap="small")

            with cols[0]:
                st.markdown(f"<div style='margin-bottom:-8px;'><b>{nm}</b></div>", unsafe_allow_html=True)

            with cols[1]:
                st.number_input(
                    f"qty_{pid}",
                    min_value=0,
                    max_value=max(0, stock_int),
                    step=1,
                    key=qty_key,
                    label_visibility="collapsed",
                    placeholder="الكمية",
                )

        submitted = st.form_submit_button(
            "✅ إضافة الأصناف المحددة للسلة",
            use_container_width=True
        )

    if submitted:
        chosen_ids = {name_to_id[nm] for nm in chosen} if chosen else set()

        # حذف الأصناف التي أزيلت من الاختيار
        for pid in list((st.session_state.prep_cart or {}).keys()):
            if pid not in chosen_ids:
                st.session_state.prep_cart.pop(pid, None)

        # تطبيق الكميات المختارة على السلة
        for nm in chosen:
            pid = name_to_id[nm]
            prod = prod_by_id.get(pid, {}) or {}
            stock_int = int(to_int(to_float(prod.get("qty_on_hand", 0)), 0))
            _apply_free_qty(pid, stock_int)

        st.rerun()
   
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
            used_price = float(cust_price_map.get(pid, base_price))

            line_total = float(used_price) * float(qty)
            total += line_total

            items.append({
                "product_id": pid,
                "product_name": pname,
                "qty": int(qty),
                "price": float(used_price),
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

        if user.get("role") == "admin":
            colA, colB, colC = st.columns(3)
        else:
            colB, colC = st.columns(2)

        if user.get("role") == "admin":
            with colA:
                if st.button("💾 حفظ كطلب مُحضّر (خصم مخزون الآن)", use_container_width=True, key="prep_save"):
                    inv = f"INV-{datetime.now(timezone(timedelta(hours=3))).strftime('%Y%m%d-%H%M%S-%f')}"
                    sale_id = inv.lower().replace(":", "").replace(" ", "_")

                    @firestore.transactional
                    def tx_prepare_and_deduct(transaction):
                        ts = now_iso()

                        prod_refs = []
                        snaps = []
                        for it in items:
                            ref = db.collection("products").document(it["product_id"])
                            snap = ref.get(transaction=transaction)
                            if not snap.exists:
                                raise ValueError(f"منتج غير موجود: {it.get('product_name','')}")
                            prod_refs.append(ref)
                            snaps.append(snap)

                        for it, snap in zip(items, snaps):
                            cur = float(to_float((snap.to_dict() or {}).get("qty_on_hand", 0)))
                            req = float(it["qty"])
                            if cur < req:
                                raise ValueError(
                                    f"المخزون غير كافي للمنتج: {it['product_name']} (المطلوب {req}, المتوفر {cur})"
                                )

                        for it, ref, snap in zip(items, prod_refs, snaps):
                            cur = float(to_float((snap.to_dict() or {}).get("qty_on_hand", 0)))
                            req = float(it["qty"])

                            transaction.update(ref, {"qty_on_hand": cur - req, "updated_at": ts})

                        sale_ref = db.collection("sales").document(sale_id)
                        transaction.set(sale_ref, {
                            "invoice_no": inv,
                            "ref": inv,
                            "customer_id": (customer_id or ""),
                            "customer_name": (customer.get("name", "") if prep_kind == "عميل" else "زائر"),
                            "seller_username": user.get("username"),
                            "distributor_id": (user.get("username") or ""),
                            "distributor_name": get_distributor_name(user.get("username") or ""),
                            "payment_type": None,
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
                            "created_at": ts,
                            "updated_at": ts,
                            "created_by": user.get("username", ""),
                        }, merge=True)

                    try:
                        tx_prepare_and_deduct(db.transaction())
                        load_products_cached.clear()
                        _get_customer_sales_for_statement.clear()
                                                  
                        moves = []
                        for it in items:
                            moves.append({
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

                        write_stock_moves_batch(moves)

                        _clear_prep_cart_and_free_qty_keys()
                        st.success("تم حفظ الطلب كمُحضّر ✅ وتم خصم المخزون ✅")
                        st.rerun()
                    except Exception as e:
                        st.error(f"فشل التحضير/الخصم: {e}")
#--style button ---red color ------------------------------------------------------------------------------                        
        components.html("""
        <script>
        function colorDirectDeliverButton() {
        const buttons = window.parent.document.querySelectorAll('button');
        buttons.forEach(btn => {
            const txt = (btn.innerText || "").trim();
            if (txt.includes("🚚 تسليم مباشر")) {
            btn.style.backgroundColor = "#ef4444";
            btn.style.color = "white";
            btn.style.border = "1px solid #ef4444";
            btn.style.fontWeight = "700";

            btn.onmouseenter = () => {
                btn.style.backgroundColor = "#dc2626";
                btn.style.border = "1px solid #dc2626";
            };
            btn.onmouseleave = () => {
                btn.style.backgroundColor = "#ef4444";
                btn.style.border = "1px solid #ef4444";
            };
            }
        });
        }

        colorDirectDeliverButton();
        setTimeout(colorDirectDeliverButton, 300);
        setTimeout(colorDirectDeliverButton, 1000);
        </script>
        """, height=0, width=0)
#--style button ---------------------------------------------------------------------------------  
        with (colB if user.get("role") == "admin" else colB):
            if st.button("🚚 تسليم مباشر (خصم + تسليم الآن)", use_container_width=True, key="prep_direct_deliver"):
                if user.get("role") == "distributor" and prep_kind == "زائر":
                    st.error("لا يمكن للموزع إنشاء طلب زائر")
                    st.stop()

                inv = f"INV-{datetime.now(timezone(timedelta(hours=3))).strftime('%Y%m%d-%H%M%S-%f')}"
                sale_id = inv.lower().replace(":", "").replace(" ", "_")

                try:
                    @firestore.transactional
                    def tx_direct_deliver(transaction):
                        ts = now_iso()

                        prod_refs = []
                        snaps = []

                        for it in items:
                            ref = db.collection("products").document(it["product_id"])
                            snap = ref.get(transaction=transaction)
                            if not snap.exists:
                                raise ValueError(f"منتج غير موجود: {it.get('product_name','')}")
                            prod_refs.append(ref)
                            snaps.append(snap)

                        for it, snap in zip(items, snaps):
                            cur = float(to_float((snap.to_dict() or {}).get("qty_on_hand", 0)))
                            req = float(it["qty"])
                            if cur < req:
                                raise ValueError(
                                    f"المخزون غير كافي للمنتج: {it['product_name']} (المطلوب {req}, المتوفر {cur})"
                                )

                        for it, ref, snap in zip(items, prod_refs, snaps):
                            cur = float(to_float((snap.to_dict() or {}).get("qty_on_hand", 0)))
                            req = float(it["qty"])
                            transaction.update(ref, {"qty_on_hand": cur - req, "updated_at": ts})
                        
                        sale_ref = db.collection("sales").document(sale_id)

                        transaction.set(sale_ref, {
                            "invoice_no": inv,
                            "ref": inv,
                            "customer_id": (customer_id or ""),
                            "customer_name": (customer.get("name", "") if prep_kind == "عميل" else "زائر"),
                            "seller_username": user.get("username"),
                            "distributor_id": (user.get("username") or ""),
                            "distributor_name": get_distributor_name(user.get("username") or ""),
                            "payment_type": None,
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
                            "created_at": ts,
                            "updated_at": ts,
                            "created_by": user.get("username", ""),
                        }, merge=True)

                    tx_direct_deliver(db.transaction())
                    load_products_cached.clear()
                    _get_customer_sales_for_statement.clear()
                    moves = []
                    for it in items:
                        moves.append({
                            "type": "sale",
                            "ref_type": "sale_direct",
                            "ref_id": sale_id,
                            "item_type": "product",
                            "item_id": it["product_id"],
                            "item_name": it.get("product_name", ""),
                            "qty_delta": -float(it["qty"]),
                            "unit": (prod_by_id.get(it["product_id"], {}) or {}).get("sale_unit", "pcs"),
                            "note": "خصم أثناء تسليم مباشر",
                            "created_by": user.get("username", ""),
                        })

                    write_stock_moves_batch(moves)
                    _clear_prep_cart_and_free_qty_keys()
                    st.session_state.deliver_target_id = sale_id
                    st.session_state.active_dialog = "deliver"
                    st.rerun()


                except Exception as e:
                    st.error(f"فشل التسليم المباشر: {e}")

        with (colC if user.get("role") == "admin" else colC):
            if st.button("🧹 تفريغ السلة", use_container_width=True, key="prep_clear"):
                _clear_prep_cart_and_free_qty_keys()
                st.rerun()

    # ---------------------------
    # Lists: Prepared + Done
    # ---------------------------
# ---------------------------
# Lists: Prepared + Done (FAST per selected customer)
# ---------------------------
    st.divider()
    st.subheader("📦 طلبات مُحضّرة جاهزة للتسليم")

    # ✅ إذا لم يتم اختيار عميل (أو زائر)، اعرض نفس السلوك القديم أو اعرض رسالة
    if prep_kind != "عميل" or not customer_id:
        st.info("اختر عميل لعرض طلباته المُحضّرة وآخر 5 فواتير مُسلّمة له.")
        prepared = []
        done = []
    else:
        # 1) كل الطلبات المُحضّرة لهذا العميل (بدون limit عادة)
        prepared_docs = (
            db.collection("sales")
            .where("active", "==", True)
            .where("status", "==", "prepared")
            .where("customer_id", "==", customer_id)
            .order_by("created_at", direction=firestore.Query.DESCENDING)
            .stream()
        )

        prepared = []
        for d in prepared_docs:
            x = d.to_dict() or {}
            x["id"] = d.id
            prepared.append(x)

        # 2) آخر 5 فواتير مُسلّمة لهذا العميل فقط
        done_docs = (
            db.collection("sales")
            .where("active", "==", True)
            .where("status", "==", "done")
            .where("customer_id", "==", customer_id)
            .order_by("delivered_at", direction=firestore.Query.DESCENDING)
            .limit(5)
            .stream()
        )

        done = []
        for d in done_docs:
            x = d.to_dict() or {}
            x["id"] = d.id
            done.append(x)

    # (اختياري) لا تحتاج sort لأن order_by موجود، لكن تركه لا يضر:
    prepared.sort(key=lambda x: (x.get("created_at") or ""), reverse=True)
    done.sort(key=lambda x: (x.get("delivered_at") or x.get("updated_at") or ""), reverse=True)

    if not prepared:
        st.info("لا يوجد طلبات مُحضّرة حالياً.")
    else:
        for o in prepared[:80]:
            sid = o["id"]
            inv = o.get("invoice_no") or o.get("ref") or sid
            cname = o.get("customer_name") or "—"
            net_v = float(to_float(o.get("net", 0)))

            row1, row2, row3 = st.columns([3.8, 1.2, 1.2])

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

            with row3:
                if st.button("❌ إلغاء الطلب", use_container_width=True, key=f"cancel_prepared_{sid}"):
                    try:
                        cancel_prepared_sale(sid, user)
                        st.success("تم إلغاء الطلب وإرجاع المخزون ✅")
                        st.rerun()
                    except Exception as e:
                        st.error(f"فشل إلغاء الطلب: {e}")

            st.divider()


    st.divider()
    st.subheader("✅ فواتير مُسلّمة (اطبع من هنا)")

    if not done:
        st.info("لا يوجد فواتير مُسلّمة حالياً.")
        return

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
        ptype = o.get("payment_type")
        paid = float(to_float(o.get("amount_paid", 0)))
        old_debt_paid = float(to_float(o.get("old_debt_paid", 0)))

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
            can_receipt = ((ptype == "cash") and (paid > 0)) or (old_debt_paid > 0)
            if st.button("🧾 قبض", use_container_width=True, key=f"done_print_receipt_{sid}", disabled=not can_receipt):
                st.session_state.last_print_sale_id = sid
                st.session_state.last_print_customer_id = None
                st.session_state._print_mode = "receipt"
                st.session_state.active_dialog = "print"
                st.rerun()

        st.divider()


#--------------------------------------------
# cancel prepered
#___________________________________________
def cancel_prepared_sale(sid: str, user: dict):
    @firestore.transactional
    def tx_cancel(transaction):
        ts = now_iso()

        sale_ref = db.collection("sales").document(sid)
        sale_snap = sale_ref.get(transaction=transaction)

        if not sale_snap.exists:
            raise ValueError("الفاتورة غير موجودة")

        sale = sale_snap.to_dict() or {}

        if sale.get("active") is not True:
            raise ValueError("الفاتورة غير فعالة")

        if sale.get("status") != "prepared":
            raise ValueError("لا يمكن إلغاء إلا الطلبات المحضّرة فقط")

        items = sale.get("items", []) or []

        # 1) اقرأ كل المنتجات أولاً
        prod_rows = []
        for it in items:
            pid = it.get("product_id")
            qty = float(to_float(it.get("qty", 0)))

            if not pid or qty <= 0:
                continue

            prod_ref = db.collection("products").document(pid)
            prod_snap = prod_ref.get(transaction=transaction)

            if not prod_snap.exists:
                raise ValueError(f"المنتج غير موجود: {it.get('product_name', '')}")

            prod_data = prod_snap.to_dict() or {}
            cur_qty = float(to_float(prod_data.get("qty_on_hand", 0)))

            prod_rows.append({
                "ref": prod_ref,
                "qty": qty,
                "cur_qty": cur_qty,
            })

        # 2) بعد انتهاء كل القراءات، ابدأي التحديثات
        for row in prod_rows:
            transaction.update(row["ref"], {
                "qty_on_hand": row["cur_qty"] + row["qty"],
                "updated_at": ts,
            })

        # 3) تحديث الفاتورة
        transaction.update(sale_ref, {
            "status": "cancelled",
            "updated_at": ts,
            "cancelled_at": ts,
            "cancelled_by": user.get("username", ""),
            "cancel_reason": "إلغاء طلب محضّر قبل التسليم",
            "stock_returned": True,
        })

    tx_cancel(db.transaction())
    load_products_cached.clear()
    _get_customer_sales_for_statement.clear()
    # تسجيل حركة المخزون بعد نجاح العملية
    sale = doc_get("sales", sid) or {}
         
    moves = []
    for it in (sale.get("items", []) or []):
        moves.append({
            "type": "sale_cancel",
            "ref_type": "sale_cancelled",
            "ref_id": sid,
            "item_type": "product",
            "item_id": it.get("product_id", ""),
            "item_name": it.get("product_name", ""),
            "qty_delta": float(to_float(it.get("qty", 0))),
            "unit": (it.get("unit") or "pcs"),
            "note": "إرجاع مخزون بسبب إلغاء طلب محضّر قبل التسليم",
            "created_by": user.get("username", ""),
        })

    write_stock_moves_batch(moves)