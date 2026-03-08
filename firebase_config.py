import os
import json
import firebase_admin
from firebase_admin import credentials, firestore


def _init_firebase():
    # لا تعيد التهيئة إذا كانت موجودة
    if firebase_admin._apps:
        return

    # 1) متغير بيئة يحتوي JSON كامل (اختياري)
    env_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if env_json:
        cred_dict = json.loads(env_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        return

    # 2) تشغيل محلي: ملف JSON بجانب app.py
    local_path = os.path.join(os.getcwd(), "serviceAccountKey.json")
    if os.path.exists(local_path):
        cred = credentials.Certificate(local_path)
        firebase_admin.initialize_app(cred)
        return

    # 3) Streamlit secrets (يستخدم فقط إذا موجود)
    try:
        import streamlit as st
        if "FIREBASE_SERVICE_ACCOUNT" in st.secrets:
            sa = dict(st.secrets["FIREBASE_SERVICE_ACCOUNT"])
            cred = credentials.Certificate(sa)
            firebase_admin.initialize_app(cred)
            return
    except Exception:
        pass

    raise RuntimeError(
        "Firebase credentials not found.\n"
        "Local: put serviceAccountKey.json next to app.py\n"
        "OR set FIREBASE_SERVICE_ACCOUNT_JSON env var\n"
        "OR (Streamlit Cloud) add FIREBASE_SERVICE_ACCOUNT in st.secrets"
    )


_init_firebase()
db = firestore.client()
