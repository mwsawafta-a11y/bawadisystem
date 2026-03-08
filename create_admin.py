import hashlib
from firebase_config import db

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

username = "ahmad"
password = "123456"
role = "distributor"   # 🔥 admin  أو  distributor

db.collection("admin_users").document(username).set({
    "username": username,
    "password_hash": hash_password(password),
    "role": role,
    "active": True
})

print(f"✅ User created: {username} | Role: {role}")
