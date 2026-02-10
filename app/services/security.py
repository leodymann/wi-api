from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def _assert_bcrypt_limit(raw: str) -> None:
    if len(raw.encode("utf-8")) > 72:
        raise ValueError("Senha muito longa. Use uma senha menor.")

def hash_password(raw: str) -> str:
    _assert_bcrypt_limit(raw)
    return pwd_context.hash(raw)

def verify_password(raw: str, hashed: str) -> bool:
    return pwd_context.verify(raw, hashed)
