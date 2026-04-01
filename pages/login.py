import streamlit as st
import hashlib
from firebase_config import db
import json
from streamlit.components.v1 import html


def hash_password(pw: str) -> str:
    return hashlib.sha256((pw or "").encode("utf-8")).hexdigest()


def logout():
    html("""
    <script>
    localStorage.removeItem("login_user");
    </script>
    """, height=0)

    st.session_state.clear()
    st.rerun()


def login():
    st.session_state.setdefault("_login_busy", False)

    # استرجاع من localStorage
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

    user = st.session_state.get("user")

    if isinstance(user, dict) and user.get("username"):
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
        st.session_state["_login_busy"] = True

        u = (username or "").strip()
        p = (password or "").strip()

        if not u or not p:
            st.error("أدخل اسم المستخدم وكلمة المرور")
            st.session_state["_login_busy"] = False
            st.stop()

        try:
            doc = db.collection("admin_users").document(u).get()

            if not doc.exists:
                st.error("اسم المستخدم غير صحيح")
                st.session_state["_login_busy"] = False
                st.stop()

            data = doc.to_dict() or {}

            if not data.get("active", False):
                st.error("الحساب موقوف")
                st.session_state["_login_busy"] = False
                st.stop()

            if data.get("password_hash") != hash_password(p):
                st.error("كلمة المرور غير صحيحة")
                st.session_state["_login_busy"] = False
                st.stop()

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

            st.session_state["_login_busy"] = False
            st.rerun()

        except Exception as e:
            st.error(f"خطأ: {e}")
            st.session_state["_login_busy"] = False

    st.stop()