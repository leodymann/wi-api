from __future__ import annotations

from decimal import Decimal
from datetime import date

from sqlalchemy import select

from app.infra.db import SessionLocal
from app.infra.models import (
    UserORM, UserRole,
    ClientORM,
    ProductORM, ProductStatus,
    PaymentType,
)
from app.services.sales_service import create_sale


def fake_hash_password(p: str) -> str:
    # igual ao router de users (MVP)
    return "hash:" + p


def main() -> None:
    db = SessionLocal()
    try:
        # ---------- USER ----------
        email = "admin@motos.com"
        user = db.scalar(select(UserORM).where(UserORM.email == email))
        if not user:
            user = UserORM(
                name="Admin",
                email=email,
                password_hash=fake_hash_password("admin123"),
                role=UserRole.ADMIN,
            )
            db.add(user)
            db.flush()
            print(f"✅ user criado id={user.id} email={user.email}")
        else:
            print(f"ℹ️ user já existe id={user.id} email={user.email}")

        # ---------- CLIENTS ----------
        clients_data = [
            ("Robisvaldo", "83987157461", "01234567890", "Rua do Barro, 10"),
            ("João Silva", "85999990000", "11122233344", "Rua A, 123"),
            ("Maria Souza", "85988887777", None, "Centro"),
        ]

        created_clients = []
        for name, phone, cpf, addr in clients_data:
            c = db.scalar(select(ClientORM).where(ClientORM.phone == phone))
            if not c:
                c = ClientORM(
                    name=name,
                    phone=phone,
                    cpf=cpf,
                    address=addr,
                    notes="seed",
                )
                db.add(c)
                db.flush()
                print(f"✅ client criado id={c.id} name={c.name}")
            created_clients.append(c)

        # ---------- PRODUCTS ----------
        products_data = [
            ("Honda", "Bros 160", 2024, None, "9C2KC0810LR000001", 0, "Vermelha", 12000, 14500),
            ("Yamaha", "Fazer 250", 2022, "ABC1D23", "9C2KC0810LR000002", 18000, "Azul", 14000, 17000),
            ("Honda", "CG 160", 2023, None, "9C2KC0810LR000003", 0, "Preta", 11500, 13900),
        ]

        created_products = []
        for brand, model, year, plate, chassi, km, color, cost, sale in products_data:
            p = db.scalar(select(ProductORM).where(ProductORM.chassi == chassi))
            if not p:
                p = ProductORM(
                    brand=brand,
                    model=model,
                    year=year,
                    plate=plate,
                    chassi=chassi,
                    km=km,
                    color=color,
                    cost_price=Decimal(str(cost)),
                    sale_price=Decimal(str(sale)),
                    status=ProductStatus.IN_STOCK,
                )
                db.add(p)
                db.flush()
                print(f"✅ product criado id={p.id} {p.brand} {p.model} chassi={p.chassi}")
            created_products.append(p)

        # ---------- SALE + PROMISSORY (exemplo) ----------
        # cria 1 venda promissória se o primeiro produto ainda estiver IN_STOCK
        prod = created_products[0]
        if prod.status == ProductStatus.IN_STOCK:
            sale, prom = create_sale(
                db,
                client_id=created_clients[0].id,
                user_id=user.id,
                product_id=prod.id,
                total=Decimal("20000.00"),
                discount=Decimal("0.00"),
                entry_amount=Decimal("2000.00"),
                payment_type=PaymentType.PROMISSORY,
                installments_count=12,
                first_due_date=None,  # 1 mês após venda
            )
            print(f"✅ sale criada id={sale.id} public_id={sale.public_id}")
            if prom:
                print(f"✅ promissory criada id={prom.id} public_id={prom.public_id}")

        db.commit()
        print("🎉 Seed finalizado!")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
