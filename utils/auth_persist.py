import json
import streamlit as st
import streamlit.components.v1 as components


def load_auth_from_browser():
    components.html(
        """
        <script>
        const authData = localStorage.getItem("app_auth");
        const url = new URL(window.parent.location);
        if (authData) {
            url.searchParams.set("auth_restore", authData);
        } else {
            url.searchParams.delete("auth_restore");
        }
        window.parent.history.replaceState({}, "", url);
        </script>
        """,
        height=0,
    )


def restore_auth_to_session():
    try:
        qp = st.query_params
        raw = qp.get("auth_restore", None)
        if not raw:
            return False

        data = json.loads(raw)

        st.session_state["logged_in"] = True
        st.session_state["user"] = data
        st.session_state["role"] = data.get("role", "")
        st.session_state["user_id"] = data.get("id", "")
        return True
    except Exception:
        return False


def save_auth_to_browser(user_data: dict):
    auth_json = json.dumps(user_data, ensure_ascii=False)
    components.html(
        f"""
        <script>
        localStorage.setItem("app_auth", {json.dumps(auth_json)});
        </script>
        """,
        height=0,
    )


def clear_auth_from_browser():
    components.html(
        """
        <script>
        localStorage.removeItem("app_auth");
        const url = new URL(window.parent.location);
        url.searchParams.delete("auth_restore");
        window.parent.history.replaceState({}, "", url);
        </script>
        """,
        height=0,
    )


def logout_user():
    for k in list(st.session_state.keys()):
        del st.session_state[k]
    clear_auth_from_browser()
    st.rerun()