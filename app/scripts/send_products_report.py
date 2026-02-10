from __future__ import annotations

import os
import time
from typing import List

from sqlalchemy import select
from dotenv import load_dotenv

from app.infra.db import SessionLocal
from app.infra.models import ProductORM, ProductStatus
from app.integrations.blibsend import send_whatsapp_text

load_dotenv()


MAX_CHARS = 3000  # seguro pra não estourar


def chunk_text(text: str, max_chars: int = MAX_CHARS) -> List[str]:
    chunks: List[str] = []
    buf: List[str] = []
    size = 0

    for line in text.splitlines(True):  # preserva \n
        if size + len(line) > max_chars and buf:
            chunks.append("".join(buf).strip())
            buf = []
            size = 0
        buf.append(line)
        size += len(line)

    if buf:
        chunks.append("".join(buf).strip())
    return [c for c in chunks if c]


def format_product(p: ProductORM) -> str:
    plate = p.plate or "SEM PLACA"
    km = p.km if p.km is not None else "-"
    return (
        f"• #{p.id} {p.brand} {p.model} {p.year} | {p.color} | KM:{km}\n"
        f"  Chassi: {p.chassi} | Placa: {plate}\n"
        f"  Status: {p.status.value} | Venda: R$ {p.sale_price} | Custo: R$ {p.cost_price}\n"
    )


def main():
    to = os.getenv("BLIBSEND_DEFAULT_TO", "").strip()
    if not to:
        raise SystemExit("Configure BLIBSEND_DEFAULT_TO no .env")

    only_in_stock = os.getenv("REPORT_ONLY_IN_STOCK", "1") == "1"
    limit = int(os.getenv("REPORT_LIMIT", "50"))

    with SessionLocal() as db:
        stmt = select(ProductORM).order_by(ProductORM.id.asc())

        if only_in_stock:
            stmt = stmt.where(ProductORM.status == ProductStatus.IN_STOCK)

        stmt = stmt.limit(limit)

        products = db.execute(stmt).scalars().all()

    if not products:
        send_whatsapp_text(to=to, body="📦 Relatório de Produtos\n\nNenhum produto encontrado.")
        print("Nada para enviar.")
        return

    header = f"📦 Relatório de Produtos ({len(products)})\n\n"
    body = header + "\n".join(format_product(p) for p in products)

    chunks = chunk_text(body, MAX_CHARS)

    for i, chunk in enumerate(chunks, start=1):
        prefix = f"[{i}/{len(chunks)}]\n"
        send_whatsapp_text(to=to, body=prefix + chunk)
        time.sleep(0.4)  # evita rajada

    print(f"OK: enviado em {len(chunks)} mensagem(ns).")


if __name__ == "__main__":
    main()
