import streamlit as st
import hashlib
import json
from firebase_config import db
from streamlit.components.v1 import html

def hash_password(pw: str) -> str:
    return hashlib.sha256((pw or "").encode("utf-8")).hexdigest()


def login(go=None):
    # محاولة استرجاع تسجيل الدخول من المتصفح
    html("""
    <script>
    const user = localStorage.getItem("login_user");
    if (user) {
        try {
            const parsed = JSON.parse(user);
            window.parent.postMessage(
                {type: "streamlit:setSessionState", key: "user", value: parsed},
                "*"
            );
            window.parent.postMessage(
                {type: "streamlit:setSessionState", key: "is_authed", value: true},
                "*"
            );
        } catch(e) {
            localStorage.removeItem("login_user");
        }
    }
    </script>
    """, height=0)

    # 1) Already logged in?
    user = st.session_state.get("user")
    if isinstance(user, dict) and user.get("username"):
        st.session_state["is_authed"] = True
        if callable(go) and st.session_state.get("page") in (None, "", "login"):
            if user.get("role") == "distributor":
                go("orders_prep")
            else:
                go("dashboard")
        return user

    st.title("تسجيل الدخول")

    username = st.text_input("اسم المستخدم", key="login_username")
    password = st.text_input("كلمة المرور", type="password", key="login_password")

    if st.button("دخول", key="login_btn", use_container_width=True):
        u = (username or "").strip()
        p = (password or "").strip()

        if not u or not p:
            st.error("أدخل اسم المستخدم وكلمة المرور")
            st.stop()

        try:
            # ابحث عن المستخدم بحقل username
            docs = list(
                db.collection("admin_users")
                .where("username", "==", u)
                .limit(1)
                .stream()
            )
        except Exception as e:
            st.error(f"تعذر الاتصال بقاعدة البيانات: {e}")
            st.exception(e)
            st.stop()

        if not docs:
            st.error("اسم المستخدم غير صحيح")
            st.stop()

        doc = docs[0]
        data = doc.to_dict() or {}

        if not data.get("active", False):
            st.error("الحساب موقوف")
            st.stop()

        saved_hash = data.get("password_hash", "")
        if saved_hash != hash_password(p):
            st.error("كلمة المرور غير صحيحة")
            st.stop()

        user = {
            "uid": doc.id,
            "username": data.get("username", u),
            "role": data.get("role", "user"),
            "distributor_id": data.get("distributor_id"),
        }

        st.session_state["user"] = user
        st.session_state["is_authed"] = True

        html(f"""
        <script>
        localStorage.setItem("login_user", {json.dumps(json.dumps(user))});
        </script>
        """, height=0)

        st.session_state.pop("login_password", None)

        if callable(go):
            if user["role"] == "distributor":
                go("orders_prep")
            else:
                go("dashboard")

        st.rerun()

    st.stop()