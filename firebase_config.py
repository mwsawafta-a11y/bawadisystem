import json
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore

if not firebase_admin._apps:

    # ✅ في Streamlit Cloud → من Secrets
    if "FIREBASE_SERVICE_ACCOUNT" in st.secrets:
        key_dict = json.loads(st.secrets["FIREBASE_SERVICE_ACCOUNT"])
        cred = credentials.Certificate(key_dict)

    # ✅ تشغيل محلي فقط (اختياري)
    else:
        cred = credentials.Certificate("serviceAccountKey.json")

    firebase_admin.initialize_app(cred)

db = firestore.client()
