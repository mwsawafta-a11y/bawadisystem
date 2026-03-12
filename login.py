import streamlit as st
import hashlib
from firebase_config import db
import json
from streamlit.components.v1 import html

def hash_password(pw: str) -> str:
    return hashlib.sha256((pw or "").encode("utf-8")).hexdigest()


def login(go=None):
    # محاولة استرجاع تسجيل الدخول من المتصفح
    

    html("""
    <script>
    const user = localStorage.getItem("login_user");
    if (user){
        window.parent.postMessage({type: "streamlit:setSessionState", key: "user", value: JSON.parse(user)}, "*");
    }
    </script>
    """, height=0)
    # =========================================================
    # 1) Already logged in?
    # =========================================================
    user = st.session_state.get("user")
    
    if isinstance(user, dict) and user.get("username"):
        if callable(go) and st.session_state.get("page") in (None, "", "login"):
            go("dashboard")
        return user

    st.title("تسجيل الدخول")

    username = st.text_input("اسم المستخدم", key="login_username")
    password = st.text_input("كلمة المرور", type="password", key="login_password")

    # =========================================================
    # 2) Submit
    # =========================================================
    if st.button("دخول", key="login_btn", use_container_width=True):
        u = (username or "").strip()
        p = (password or "").strip()

        if not u or not p:
            st.error("أدخل اسم المستخدم وكلمة المرور")
            st.stop()

        # 🔥 القراءة من users بدل admin_users
        try:
            doc = db.collection("admin_users").document(u).get()
        except Exception as e:
            st.error(f"تعذر الاتصال بقاعدة البيانات: {e}")
            st.stop()

        if not getattr(doc, "exists", False):
            st.error("اسم المستخدم غير صحيح")
            st.stop()

        data = doc.to_dict() or {}

        if not data.get("active", False):
            st.error("الحساب موقوف")
            st.stop()

        if data.get("password_hash") != hash_password(p):
            st.error("كلمة المرور غير صحيحة")
            st.stop()

        # =========================================================
        # 3) Success
        # =========================================================
        user = {
            "username": u,
            "role": data.get("role", "user"),
            "distributor_id": data.get("distributor_id")  # 🔥 مهم
        }

        st.session_state["user"] = user
        st.session_state["is_authed"] = True
        st.components.v1.html(f"""
        <script>
        localStorage.setItem("login_user", '{json.dumps(user)}');
        </script>
        """, height=0)
        # تنظيف الحقول
        st.session_state.pop("login_password", None)

        # 🔥 توجيه حسب الدور
        if callable(go):
            if user["role"] == "distributor":
                go("orders_prep")
            else:
                go("dashboard")

        st.rerun()

    # =========================================================
    # 4) Stop app until logged in
    # =========================================================
    st.stop()
