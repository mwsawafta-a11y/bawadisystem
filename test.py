import streamlit as st
st.write("HOST:", st.context.headers.get("host") if hasattr(st, "context") else "no context")
st.write("SECRETS keys:", list(st.secrets.keys()) if hasattr(st, "secrets") else "no secrets")