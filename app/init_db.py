from app.infra.db import engine
from app.infra.models import Base
from sqlalchemy.orm import Session
from app.infra.models import UserORM
from app.services.security import hash_password  # ou sua função

def main():
    Base.metadata.create_all(bind=engine)
    print("Tabelas criadas!")

if __name__ == "__main__":
    main()
def ensure_admin(db: Session):
    email = "admin@admin.com"
    user = db.query(UserORM).filter(UserORM.email == email).first()
    if user:
        return

    user = UserORM(
        name="Admin",
        email=email,
        password_hash=hash_password("admin123"),
        role="ADMIN",
    )
    db.add(user)
    db.commit()