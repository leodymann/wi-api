from datetime import datetime, timedelta, timezone
from jose import jwt

from app.config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRES_MINUTES

def create_access_token(*, sub: str, role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": sub,               # user_id (string)
        "role": role,             # ADMIN/STAFF
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=JWT_EXPIRES_MINUTES)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
