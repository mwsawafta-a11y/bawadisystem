import os
import json
import firebase_admin
from firebase_admin import credentials, firestore

def _init():
    if firebase_admin._apps:
        return

    # ✅ 1) إذا في متغير بيئة (مفيد للمستقبل)
    env_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if env_json:
        cred = credentials.Certificate(json.loads(env_json))
        firebase_admin.initialize_app(cred)
        return

    # ✅ 2) تشغيل محلي: ملف json موجود عندك (ولا ترفعه على GitHub)
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
        return

    # ✅ 3) على Streamlit Cloud: استخدم Secrets (لكن فقط هناك)
    try:
        import streamlit as st
        sa = dict(st.secrets["FIREBASE_SERVICE_ACCOUNT"])
        cred = credentials.Certificate(sa)
        firebase_admin.initialize_app(cred)
        return
    except Exception as e:
        raise RuntimeError(
            "Firebase credentials not found. "
            "Local: put serviceAccountKey.json next to app.py "
            "OR create .streamlit/secrets.toml. "
            f"Details: {e}"
        )

_init()
db = firestore.client()
