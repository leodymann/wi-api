from pydantic_settings import BaseSettings, SettingsConfigDict
import os

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-me")
JWT_ALGORITHM = "HS256"
JWT_EXPIRES_MINUTES = int(os.getenv("JWT_EXPIRES_MINUTES", "60"))


def normalize_db_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL não configurada.")

    # Railway/Heroku: postgres://...
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)

    # Railway: postgresql://... (sem driver)
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return url


class Settings(BaseSettings):
    DATABASE_URL: str

    model_config = SettingsConfigDict(
        env_file=".env",         # local
        env_ignore_empty=True,   # evita sobrescrever com vazio
        extra="ignore",
    )

    def __init__(self, **values):
        super().__init__(**values)
        self.DATABASE_URL = normalize_db_url(self.DATABASE_URL)


settings = Settings()
