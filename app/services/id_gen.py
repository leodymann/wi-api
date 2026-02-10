import secrets
ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"

def generate_public_id(prefix: str, length: int = 8) -> str:
    token = "".join(secrets.choice(ALPHABET) for _ in range(length))
    return f"{prefix}-{token}"
