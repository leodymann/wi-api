# scripts/create_admin.py
from app.infra.db import SessionLocal
from app.init_db import ensure_admin

db = SessionLocal()
ensure_admin(db)
db.close()
print("Admin criado")
