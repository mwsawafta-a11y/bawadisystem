import streamlit as st
from pages.login import login

from pages.customers_page import customers_page
from pages.distributors_page import distributors_page
from pages.inventory_page import inventory_page
from pages.orders_prep_page import orders_prep_page
from pages.orders_archive_page import orders_archive_page

st.set_page_config(
    page_title="مخابز البوادي",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =========================================================
# Global UI/CSS (Hide + Responsive + Fix top spacing)
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
hr, .stDivider { margin: 0.6rem 0 !important; }

div[data-testid="column"] button{
  height: 44px !important;
  border-radius: 14px !important;
  font-weight: 800 !important;
  border: 1px solid rgba(0,0,0,0.08) !important;
}

div[data-testid="stDataFrame"]{
  width: 100%;
  overflow-x: auto;
}

details summary { font-weight: 800; }

@media (max-width: 768px){
  section.main > div.block-container{
    padding-top: 0.1rem !important;
    padding-left: 0.6rem !important;
    padding-right: 0.6rem !important;
  }

  div[data-testid="stHorizontalBlock"]{
    flex-wrap: wrap !important;
    gap: 0.6rem !important;
  }

  div[data-testid="stHorizontalBlock"] > div:has(div[data-testid="stMetric"]){
    flex: 0 0 calc(50% - 0.6rem) !important;
    width: calc(50% - 0.6rem) !important;
    min-width: calc(50% - 0.6rem) !important;
  }

  div[data-testid="stHorizontalBlock"]:has(div[data-testid="stDownloadButton"]),
  div[data-testid="stHorizontalBlock"]:has(div[data-testid="stButton"]){
    justify-content: center !important;
  }

  div[data-testid="stHorizontalBlock"] > div:has(div[data-testid="stDownloadButton"]),
  div[data-testid="stHorizontalBlock"] > div:has(div[data-testid="stButton"]){
    flex: 0 0 calc(50% - 0.6rem) !important;
    width: calc(50% - 0.6rem) !important;
    min-width: calc(50% - 0.6rem) !important;
  }

  div[data-testid="stDownloadButton"] button,
  div[data-testid="stButton"] button{
    width: 100% !important;
    height: 46px !important;
  }

  div[data-testid="stMetric"] *{
    font-size: 0.95rem !important;
  }

  div[data-testid="stDataFrame"] *{
    font-size: 0.90rem !important;
  }
}
</style>
"""
st.markdown(hide_all_streamlit, unsafe_allow_html=True)

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
if st.session_state.get("user") is None:
    login(go)
    st.stop()

user = st.session_state.user
role = user.get("role")

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
        role = user.get("role")

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
    if user.get("role") == "admin":
        customers_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

elif st.session_state.page == "inventory":
    if user.get("role") == "admin":
        inventory_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

elif st.session_state.page == "orders_prep":
    if user.get("role") in ["admin", "distributor"]:
        orders_prep_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

elif st.session_state.page == "distributors":
    if user.get("role") == "admin":
        distributors_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

elif st.session_state.page == "orders_archive":
    if user.get("role") == "admin":
        orders_archive_page(go, user)
    else:
        st.error("ليس لديك صلاحية الوصول")

else:
    st.session_state.page = "dashboard"
    st.rerun()