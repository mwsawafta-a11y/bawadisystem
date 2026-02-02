import streamlit as st
import hashlib
from firebase_config import db

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

def login(go=None):
    # ✅ إذا المستخدم مسجل دخول بالفعل، لا تعرض صفحة الدخول
    if st.session_state.get("user") and st.session_state.get("user", {}).get("username"):
        if callable(go):
            go("dashboard")
        st.stop()

    st.title("تسجيل دخول الأدمن")

    username = st.text_input("اسم المستخدم", key="login_username")
    password = st.text_input("كلمة المرور", type="password", key="login_password")

    if st.button("دخول", key="login_btn"):
        u = (username or "").strip()
        p = (password or "").strip()

        if not u or not p:
            st.error("أدخل اسم المستخدم وكلمة المرور")
            return

        doc = db.collection("admin_users").document(u).get()

        if not doc.exists:
            st.error("اسم المستخدم غير صحيح")
            return

        data = doc.to_dict() or {}

        if not data.get("active", False):
            st.error("الحساب موقوف")
            return

        if data.get("password_hash") != hash_password(p):
            st.error("كلمة المرور غير صحيحة")
            return

        st.session_state["user"] = {
            "username": u,
            "role": data.get("role", "user"),
        }
        st.session_state["is_authed"] = True

        # ✅ لا تعدّل قيمة widget مباشرة بعد إنشائها
        st.session_state.pop("login_password", None)
        # اختياري: مسح الاسم أيضًا
        # st.session_state.pop("login_username", None)

        if callable(go):
            go("dashboard")
            st.stop()

        st.rerun()
