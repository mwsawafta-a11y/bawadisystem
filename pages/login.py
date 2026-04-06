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

    # 🔥 استرجاع المستخدم من localStorage
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

    # إذا مسجل دخول
    if isinstance(user, dict) and user.get("username"):
        return user

    # 🔴 واجهة تسجيل الدخول
    st.title("تسجيل الدخول")

    username = st.text_input("اسم المستخدم", key="login_username")
    password = st.text_input("كلمة المرور", type="password", key="login_password")

    # 🔥 الزر بدون أي busy أو disabled
    if st.button("دخول", key="login_btn", use_container_width=True):

        u = (username or "").strip()
        p = (password or "").strip()

        if not u or not p:
            st.error("أدخل اسم المستخدم وكلمة المرور")
            st.stop()

        try:
            doc = db.collection("admin_users").document(u).get()

            if not doc.exists:
                st.error("اسم المستخدم غير صحيح")
                st.stop()

            data = doc.to_dict() or {}

            if not data.get("active", False):
                st.error("الحساب موقوف")
                st.stop()

            if data.get("password_hash") != hash_password(p):
                st.error("كلمة المرور غير صحيحة")
                st.stop()

            # 🔥 إنشاء user session
            user = {
                "username": u,
                "role": data.get("role", "user"),
                "distributor_id": data.get("distributor_id")
            }

            st.session_state["user"] = user
            st.session_state["is_authed"] = True

            # حفظ في localStorage
            html(f"""
            <script>
            localStorage.setItem("login_user", {json.dumps(user)});
            </script>
            """, height=0)

            # إعادة تشغيل مرة واحدة فقط بعد النجاح
            st.rerun()

        except Exception as e:
            st.error(f"خطأ: {e}")
            st.stop()

    st.stop()