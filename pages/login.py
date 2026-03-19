import streamlit as st
import hashlib
from firebase_config import db
import json
from streamlit.components.v1 import html


def hash_password(pw: str) -> str:
    return hashlib.sha256((pw or "").encode("utf-8")).hexdigest()


def login(go=None):
    st.session_state.setdefault("_login_busy", False)

    # محاولة استرجاع تسجيل الدخول من localStorage
    html("""
    <script>
    const user = localStorage.getItem("login_user");
    if (user && !window.__loginUserRestoredOnce) {
        window.__loginUserRestoredOnce = true;
        window.parent.postMessage({
            type: "streamlit:setSessionState",
            key: "user",
            value: JSON.parse(user)
        }, "*");
    }
    </script>
    """, height=0)

    # إذا تم الاسترجاع بالفعل
    user = st.session_state.get("user")
    if isinstance(user, dict) and user.get("username"):
        if callable(go) and st.session_state.get("page") in (None, "", "login"):
            if user.get("role") == "distributor":
                go("orders_prep")
            else:
                go("dashboard")
        return user

    st.title("تسجيل الدخول")

    username = st.text_input("اسم المستخدم", key="login_username")
    password = st.text_input("كلمة المرور", type="password", key="login_password")

    if st.button(
        "دخول",
        key="login_btn",
        use_container_width=True,
        disabled=st.session_state.get("_login_busy", False)
    ):
        if st.session_state.get("_login_busy", False):
            st.warning("⏳ جاري تسجيل الدخول...")
            st.stop()

        st.session_state["_login_busy"] = True

        try:
            u = (username or "").strip()
            p = (password or "").strip()

            if not u or not p:
                st.error("أدخل اسم المستخدم وكلمة المرور")
                return

            try:
                doc = db.collection("admin_users").document(u).get()
            except Exception as e:
                st.error(f"تعذر الاتصال بقاعدة البيانات: {e}")
                return

            if not getattr(doc, "exists", False):
                st.error("اسم المستخدم غير صحيح")
                return

            data = doc.to_dict() or {}

            if not data.get("active", False):
                st.error("الحساب موقوف")
                return

            if data.get("password_hash") != hash_password(p):
                st.error("كلمة المرور غير صحيحة")
                return

            user = {
                "username": u,
                "role": data.get("role", "user"),
                "distributor_id": data.get("distributor_id")
            }

            st.session_state["user"] = user
            st.session_state["is_authed"] = True

            html(f"""
            <script>
            localStorage.setItem("login_user", {json.dumps(user)});
            </script>
            """, height=0)

            st.session_state.pop("login_password", None)

            if callable(go):
                if user["role"] == "distributor":
                    go("orders_prep")
                else:
                    go("dashboard")
                return

            st.rerun()

        finally:
            st.session_state["_login_busy"] = False

    st.stop()