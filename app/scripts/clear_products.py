from __future__ import annotations

from sqlalchemy import text
from app.infra.db import engine

SQL = """
TRUNCATE TABLE product_images, products RESTART IDENTITY CASCADE;
"""

def main() -> None:
    with engine.begin() as conn:
        conn.execute(text(SQL))
    print("OK: products + product_images apagados.")

if __name__ == "__main__":
    main()
