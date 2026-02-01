import firebase_admin
from firebase_admin import credentials, firestore
import streamlit as st

if not firebase_admin._apps:
    # ✅ Secrets عندك dict (TOML table) لذا نأخذها مباشرة
    sa = dict(st.secrets["FIREBASE_SERVICE_ACCOUNT"])
    cred = credentials.Certificate(sa)
    firebase_admin.initialize_app(cred)

db = firestore.client()