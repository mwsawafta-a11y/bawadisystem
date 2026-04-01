import streamlit as st
from pages.login import login

from pages.customers_page import customers_page
from pages.distributors_page import distributors_page
from pages.inventory_page import inventory_page
from pages.orders_prep_page import orders_prep_page
from pages.orders_archive_page import orders_archive_page

# ✅ لازم يكون أول شيء
st.set_page_config(
    page_title="مخابز البوادي",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =========================================================
# CSS
# =========================================================
hide_all_streamlit = """
<style>
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}
.stDeployButton {display:none;}
[data-testid="stDecoration"] {display:none;}

html, body, [data-testid="stAppViewContainer"], .stApp {
  direction: rtl;
  text-align: right;
}

section.main > div.block-container{
  max-width: 1200px;
  padding-top: 0.2rem !important;
  padding-bottom: 1.6rem !important;
  padding-left: 1rem !important;
  padding-right: 1rem !important;
}

h1, h2, h3 { margin-top: 0.2rem !important; }

div[data-testid="column"] button{
  height: 44px !important;
  border-radius: 14px !important;
  font-weight: 800 !important;
}
</style>
"""
st.markdown(hide_all_streamlit, unsafe_allow_html=True)

# =========================================================
# Session
# =========================================================
st.session_state.setdefault("user", None)
st.session_state.setdefault("page", "dashboard")

def go(p: str):
    st.session_state.page = p

# =========================================================
# Login (🔥 تم إصلاحه)
# =========================================================
if st.session_state.get("user") is None:
    user = login()

    if not user:
        st.stop()

    st.session_state.user = user

user = st.session_state.get("user")
role = user.get("role")

# توجيه أول مرة
if st.session_state.page in (None, "", "login"):
    if role == "distributor":
        st.session_state.page = "orders_prep"
    else:
        st.session_state.page = "dashboard"

if role not in ["admin", "distributor"]:
    st.error("ليس لديك صلاحية الوصول")
    st.stop()

# =========================================================
# Dashboard
# =========================================================
if st.session_state.page == "dashboard":
    st.markdown("<h2 style='text-align:center;'>لوحة التحكم</h2>", unsafe_allow_html=True)
    st.caption(f"مرحبًا: {user.get('username','')}")

    left, center, right = st.columns([1.2, 2.2, 1.2])

    with center:
        if role == "admin":
            if st.button("👥 العملاء", use_container_width=True):
                go("customers")

            if st.button("📦 إدارة المستودع", use_container_width=True):
                go("inventory")

            if st.button("📁 أرشيف الفواتير", use_container_width=True):
                go("orders_archive")

            if st.button("🚚 الموزعين", use_container_width=True):
                go("distributors")

        if role in ["admin", "distributor"]:
            if st.button("🧑‍🍳 تحضير الأوردرات", use_container_width=True):
                go("orders_prep")

        if st.button("🚪 تسجيل الخروج", use_container_width=True):
            st.components.v1.html("""
            <script>
            localStorage.removeItem("login_user");
            </script>
            """, height=0)

            st.session_state.clear()
            st.rerun()

# =========================================================
# Pages
# =========================================================
elif st.session_state.page == "customers":
    if role == "admin":
        customers_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

elif st.session_state.page == "inventory":
    if role == "admin":
        inventory_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

elif st.session_state.page == "orders_prep":
    if role in ["admin", "distributor"]:
        orders_prep_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

elif st.session_state.page == "distributors":
    if role == "admin":
        distributors_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

elif st.session_state.page == "orders_archive":
    if role == "admin":
        orders_archive_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

else:
    st.session_state.page = "dashboard"
    st.rerun()