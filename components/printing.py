import base64
from pathlib import Path
import streamlit.components.v1 as components
from datetime import datetime, timezone, timedelta
from utils.helpers import to_float


# --------------------------------
# Helpers
# -------------------------------
def _money(x):
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "0.00"


def _dt_short(x):
    return (x or "")[:19].replace("T", " ")


def _now_dt():
    return datetime.now(timezone(timedelta(hours=3))).strftime("%Y-%m-%d %H:%M:%S")


def _get_logo_base64():
    try:
        logo_path = Path(__file__).resolve().parent.parent / "assets" / "logo.png"
        if not logo_path.exists():
            return ""
        return base64.b64encode(logo_path.read_bytes()).decode("utf-8")
    except Exception:
        return ""


def _logo_html():
    logo_b64 = _get_logo_base64()
    if not logo_b64:
        return ""
    return f"""
    <div class="logo-wrap">
        <img class="logo" src="data:image/png;base64,{logo_b64}" alt="logo">
    </div>
    """


def _base_css(paper="80mm"):
    if paper == "a4":
        return """
        *{box-sizing:border-box}
        body{
            font-family:Arial,sans-serif;
            direction:rtl;
            margin:0;
            padding:0;
            background:#fff;
            color:#000;
            font-weight:700;
        }
        .wrap{
            width:190mm;
            margin:0 auto;
            padding:10mm;
        }
        .center{text-align:center}
        .muted{color:#666}
        .logo-wrap{
            text-align:center;
            margin-bottom:6px;
        }
        .logo{
            max-width:90px;
            max-height:90px;
            object-fit:contain;
        }
        hr{
            border:none;
            border-top:1px dashed #999;
            margin:8px 0;
        }
        .sumrow{
            display:flex;
            justify-content:space-between;
            gap:8px;
            margin-top:5px;
            font-size:14px;
        }
        .sumrow span:last-child{
            text-align:left;
        }
        .title{
            font-size:22px;
            font-weight:800;
        }
        .badge{
            display:inline-block;
            border:1px solid #bbb;
            border-radius:999px;
            padding:5px 12px;
            margin-top:6px;
            font-size:14px;
        }
        table{
            width:100%;
            border-collapse:collapse;
            margin-top:6px;
        }
        th,td{
            padding:7px 4px;
            border-bottom:1px solid #ddd;
            text-align:right;
            vertical-align:top;
            font-size:14px;
        }
        th{
            font-size:13px;
            background:#fafafa;
        }
        td.name{
            width:40%;
            word-break:break-word;
        }
        td.qty, td.price, td.tot{
            white-space:nowrap;
            text-align:center;
        }
        .grand{
            font-size:18px;
            font-weight:800;
        }
        .btnbar{
            margin-top:12px;
            display:flex;
            gap:8px;
        }
        button{
            width:100%;
            padding:12px;
            font-size:15px;
            font-weight:800;
            cursor:pointer;
            border:none;
            border-radius:10px;
            background:#111827;
            color:#fff;
        }
        @media print{
            .btnbar{display:none}
            body{-webkit-print-color-adjust:exact; print-color-adjust:exact;}
        }
        """
    else:
        return """
        *{box-sizing:border-box}
        body{
            font-family:Arial,sans-serif;
            direction:rtl;
            margin:0 auto;
            padding:0;
            width:72mm;
            background:#fff;
            color:#000;
            font-weight:700;
        }
        .wrap{
            width:68mm;
            margin:0 auto;
            padding:2.5mm 1.8mm;
        }
        .center{text-align:center}
        .muted{color:#666}
        .logo-wrap{
            text-align:center;
            margin-bottom:5px;
        }
        .logo{
            max-width:52px;
            max-height:52px;
            object-fit:contain;
        }
        hr{
            border:none;
            border-top:1px dashed #999;
            margin:6px 0;
        }
        .sumrow{
            display:flex;
            justify-content:space-between;
            gap:6px;
            margin-top:4px;
            font-size:12px;
            line-height:1.5;
        }
        .sumrow span:first-child{
            flex:1;
        }
        .sumrow span:last-child{
            min-width:20mm;
            text-align:left;
            white-space:nowrap;
        }
        .title{
            font-size:18px;
            font-weight:800;
            line-height:1.3;
        }
        .badge{
            display:inline-block;
            border:1px solid #bbb;
            border-radius:999px;
            padding:4px 8px;
            margin-top:5px;
            font-size:11px;
        }
        table{
            width:100%;
            border-collapse:collapse;
            margin-top:4px;
        }
        th,td{
            padding:4px 2px;
            border-bottom:1px solid #ddd;
            text-align:right;
            vertical-align:top;
            font-size:11px;
        }
        th{
            font-size:10px;
        }
        td.name{
            width:40%;
            word-break:break-word;
            line-height:1.35;
        }
        td.qty, td.price, td.tot{
            white-space:nowrap;
            text-align:center;
            font-size:10.5px;
        }
        .grand{
            font-size:14px;
            font-weight:800;
        }
        .btnbar{
            margin-top:10px;
            display:flex;
            gap:6px;
        }
        button{
            width:100%;
            padding:10px 8px;
            font-size:13px;
            font-weight:800;
            cursor:pointer;
            border:none;
            border-radius:8px;
            background:#111827;
            color:#fff;
        }
        @media print{
            .btnbar{display:none}
            body{
                width:72mm !important;
                margin:0 auto !important;
                -webkit-print-color-adjust:exact;
                print-color-adjust:exact;
            }
            .wrap{
                width:68mm !important;
                margin:0 auto !important;
            }
            @page{
                size:80mm auto;
                margin:2mm;
            }
        }
        """


def _print_script(auto_print=False):
    return f"""
    <script>
      function doPrint() {{
        try {{
          window.focus();
          setTimeout(() => {{
            window.print();
          }}, 250);
        }} catch(e) {{
          console.log(e);
        }}
      }}

      {"window.onload = function(){ setTimeout(doPrint, 350); };" if auto_print else ""}
    </script>
    """


# ---------------------------
# Invoice
# ---------------------------
def build_invoice_html(
    sale: dict,
    customer: dict = None,
    company_name="مخابز البوادي",
    paper="80mm",
    auto_print=False
):
    customer = customer or {}
    items = sale.get("items", []) or []
    logo_html = _logo_html()

    created = _dt_short(sale.get("created_at") or sale.get("updated_at"))
    delivered = _dt_short(sale.get("delivered_at"))
    dt = delivered if delivered else created
    if not dt:
        dt = _now_dt()

    invoice_no = sale.get("invoice_no") or sale.get("ref") or sale.get("id") or ""
    cust_name = sale.get("customer_name") or customer.get("name") or "—"
    phone = customer.get("phone", "")
    seller_username = (
        sale.get("distributor_name")
        or sale.get("seller_username", "")
    )

    total = float(to_float(sale.get("total", 0)))
    discount = float(to_float(sale.get("discount", 0)))
    net = float(total) - float(discount)

    ptype = sale.get("payment_type")

    paid = float(to_float(sale.get("amount_paid", 0)))
    old_debt_paid = float(to_float(sale.get("old_debt_paid", 0)))
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
        qty = float(to_float(it.get("qty", 0)))
        price = float(to_float(it.get("price", 0)))
        line_total = float(to_float(it.get("total", qty * price)))

        rows_html += f"""
        <tr>
          <td class="name">{pname}</td>
          <td class="qty">{int(qty)}</td>
          <td class="price">{_money(price)}</td>
          <td class="tot">{_money(line_total)}</td>
        </tr>
        """

    pay_lines = ""

    if ptype == "cash":
        pay_lines += f"""
        <div class="sumrow">
          <span>المسدّد لهذه الفاتورة:</span>
          <span><b>{_money(paid)}</b></span>
        </div>
        """
        if extra_credit > 0:
            pay_lines += f"""
            <div class="sumrow">
              <span>زيادة كرصد للعميل:</span>
              <span><b>{_money(extra_credit)}</b></span>
            </div>
            """
        if unpaid > 0:
            pay_lines += f"""
            <div class="sumrow">
              <span>متبقي من هذه الفاتورة:</span>
              <span><b>{_money(unpaid)}</b></span>
            </div>
            """

    if old_debt_paid > 0:
        pay_lines += f"""
        <div class="sumrow">
          <span>ذمم سابقة مسددة:</span>
          <span><b>{_money(old_debt_paid)}</b></span>
        </div>
        """

    if total_collected > 0:
        pay_lines += f"""
        <div class="sumrow grand">
          <span>إجمالي المقبوض:</span>
          <span><b>{_money(total_collected)}</b></span>
        </div>
        """

    if final_due > 0:
        pay_lines += f"""
        <div class="sumrow">
          <span>إجمالي الذمم المستحقة:</span>
          <span><b>{_money(final_due)}</b></span>
        </div>
        """

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
{_base_css(paper)}
</style>
{_print_script(auto_print=auto_print)}
</head>
<body>
<div class="wrap">

  <div class="center">
    {logo_html}
    <div class="title">{company_name}</div>
    <div class="badge">{header_type}</div>
  </div>

  <hr>

  <div class="sumrow"><span>رقم الفاتورة:</span><span>{invoice_no}</span></div>
  <div class="sumrow"><span>التاريخ:</span><span>{dt}</span></div>
  <div class="sumrow"><span>العميل:</span><span>{cust_name}</span></div>
  <div class="sumrow"><span>الموزع:</span><span>{seller_username or '—'}</span></div>
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
  <div class="sumrow grand"><span>الصافي:</span><span>{_money(net)}</span></div>

  {pay_lines}

  <hr>
  <div class="center">شكراً لزيارتكم ❤️</div>

  <div class="btnbar">
    <button onclick="doPrint()">🖨️ طباعة الآن</button>
  </div>

</div>
</body>
</html>
"""
    return html


# ---------------------------
# Receipt
# ---------------------------
def build_receipt_html(
    sale: dict,
    customer: dict = None,
    company_name="مخابز البوادي",
    paper="80mm",
    auto_print=False
):
    customer = customer or {}
    logo_html = _logo_html()

    created = _dt_short(sale.get("created_at") or sale.get("updated_at"))
    delivered = _dt_short(sale.get("delivered_at"))
    dt = delivered if delivered else created
    if not dt:
        dt = _now_dt()

    invoice_no = sale.get("invoice_no") or sale.get("ref") or sale.get("id") or ""
    cust_name = sale.get("customer_name") or customer.get("name") or "—"

    total = float(to_float(sale.get("total", 0)))
    discount = float(to_float(sale.get("discount", 0)))
    net = float(total) - float(discount)

    paid = float(to_float(sale.get("amount_paid", 0)))
    old_debt_paid = float(to_float(sale.get("old_debt_paid", 0)))
    old_debt_remaining = float(to_float(sale.get("old_debt_remaining", 0)))
    final_due = float(to_float(sale.get("final_due", 0)))
    extra_credit = float(to_float(sale.get("extra_credit", 0)))
    unpaid = float(to_float(sale.get("unpaid_debt", 0)))

    total_collected = paid + old_debt_paid

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Receipt</title>
<style>
{_base_css(paper)}
</style>
{_print_script(auto_print=auto_print)}
</head>
<body>
  <div class="wrap">
    <div class="center">
      {logo_html}
      <div class="title">{company_name}</div>
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
    {f"<div class='sumrow grand'><span>إجمالي المقبوض:</span><span><b>{_money(total_collected)}</b></span></div>" if total_collected > 0 else ""}
    {f"<div class='sumrow'><span>زيادة كرصد للعميل:</span><span><b>{_money(extra_credit)}</b></span></div>" if extra_credit > 0 else ""}
    {f"<div class='sumrow'><span>متبقي من هذه الفاتورة:</span><span><b>{_money(unpaid)}</b></span></div>" if unpaid > 0 else ""}

    <hr/>
    <div class="center muted">هذا الإيصال يثبت عملية الدفع/الرصيد.</div>

    <div class="btnbar">
      <button onclick="doPrint()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html


# ---------------------------
# Debt only
# ---------------------------
def build_debt_only_invoice_html(
    customer: dict,
    company_name="مخابز البوادي",
    paper="80mm",
    auto_print=False
):
    customer = customer or {}
    logo_html = _logo_html()
    cust_name = customer.get("name") or "—"
    phone = customer.get("phone") or ""

    bal = float(to_float(customer.get("balance", 0)))
    debt = bal if bal > 0 else 0.0
    dt = _now_dt()

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
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Debt Only Invoice</title>
<style>
{_base_css(paper)}
</style>
{_print_script(auto_print=auto_print)}
</head>
<body>
  <div class="wrap">
    <div class="center">
      {logo_html}
      <div class="title">{company_name}</div>
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
      <button onclick="doPrint()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html


# ---------------------------
# Statement
# ---------------------------
def _pick_dt_for_sort(s: dict):
    return (s.get("delivered_at") or s.get("updated_at") or s.get("created_at") or "")


def _calc_balance_delta_from_sale(s: dict) -> float:
    ptype = s.get("payment_type")
    net = float(to_float(s.get("net", 0)))
    unpaid = float(to_float(s.get("unpaid_debt", 0)))
    extra = float(to_float(s.get("extra_credit", 0)))

    if ptype == "credit":
        return +net
    if ptype == "cash":
        return (unpaid - extra)
    return 0.0


def build_customer_statement_html(
    customer: dict,
    sales: list,
    company_name="مخابز البوادي",
    paper="80mm",
    max_rows=30,
    auto_print=False
):
    customer = customer or {}
    logo_html = _logo_html()
    cust_name = customer.get("name") or "—"
    phone = customer.get("phone") or ""
    balance_now = float(to_float(customer.get("balance", 0)))

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
          <td>{dt}</td>
          <td><b>{inv}</b></td>
          <td>{stxt}</td>
          <td>{ptxt}</td>
          <td>{_money(net)}</td>
          <td>{_money(paid)}</td>
          <td>{_money(unpaid)}</td>
          <td>{_money(extra)}</td>
          <td>{_money(delta)}</td>
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
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Customer Statement</title>
<style>
{_base_css(paper)}
</style>
{_print_script(auto_print=auto_print)}
</head>
<body>
  <div class="wrap">
    <div class="center">
      {logo_html}
      <div class="title">{company_name}</div>
      <div class="badge">كشف حساب عميل (مختصر)</div>
    </div>

    <hr/>

    <div class="sumrow"><span>العميل:</span><span><b>{cust_name}</b></span></div>
    {f"<div class='sumrow'><span>هاتف:</span><span>{phone}</span></div>" if phone else ""}
    <div class="sumrow"><span>الرصيد الحالي:</span><span><b>{bal_label}: {bal_value}</b></span></div>
    <div class="sumrow"><span>تاريخ الطباعة:</span><span>{_now_dt()}</span></div>

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
      * أثر الرصيد: ذمم = +صافي، نقدي = +المتبقي - الزيادة كرصد.
    </div>

    <div class="btnbar">
      <button onclick="doPrint()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html


# ---------------------------
# Debt payment receipt
# ---------------------------
def build_debt_payment_receipt_html(
    customer: dict,
    amount: float,
    remaining: float,
    company_name="البوادي",
    paper="80mm",
    auto_print=False
):
    customer = customer or {}
    logo_html = _logo_html()
    cust_name = customer.get("name") or "—"
    phone = customer.get("phone") or ""
    dt = _now_dt()

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Debt Payment Receipt</title>
<style>
{_base_css(paper)}
</style>
{_print_script(auto_print=auto_print)}
</head>
<body>
  <div class="wrap">
    <div class="center">
      {logo_html}
      <div class="title">{company_name}</div>
      <div class="badge">سند قبض تسديد ذمم</div>
    </div>

    <hr/>

    <div class="sumrow"><span>التاريخ:</span><span>{dt}</span></div>
    <div class="sumrow"><span>العميل:</span><span><b>{cust_name}</b></span></div>
    {f"<div class='sumrow'><span>هاتف:</span><span>{phone}</span></div>" if phone else ""}

    <hr/>

    <div class="sumrow"><span>المبلغ المقبوض:</span><span><b>{_money(amount)}</b></span></div>
    <div class="sumrow"><span>المتبقي من الذمم:</span><span><b>{_money(remaining)}</b></span></div>

    <hr/>
    <div class="center muted">هذا السند يثبت تسديد ذمم للعميل.</div>

    <div class="btnbar">
      <button onclick="doPrint()">🖨️ طباعة الآن</button>
    </div>
  </div>
</body>
</html>
"""
    return html


# ---------------------------
# Render in Streamlit
# ---------------------------
def show_print_html(html, height=1100):
    components.html(html, height=height, scrolling=True)