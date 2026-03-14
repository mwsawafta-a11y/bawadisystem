from datetime import datetime, timezone, timedelta

def now_iso():
    tz = timezone(timedelta(hours=3))  # Jordan
    return datetime.now(tz).isoformat()

def to_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def to_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default