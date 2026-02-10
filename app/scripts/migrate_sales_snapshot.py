from __future__ import annotations

from sqlalchemy import text
from app.infra.db import engine

SQL = """
ALTER TABLE promissories
  ADD COLUMN IF NOT EXISTS daily_late_fee numeric(12,2),
  ADD COLUMN IF NOT EXISTS late_penalty numeric(12,2),
  ADD COLUMN IF NOT EXISTS late_penalty_percent numeric(7,4);

"""

def main():
    with engine.begin() as conn:
        conn.execute(text(SQL))
    print("OK: sales alterada com colunas de snapshot.")

if __name__ == "__main__":
    main()
