from __future__ import annotations

from decimal import Decimal
from typing import Optional, List, Sequence

from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form, Depends
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.api.deps import DBSession
from app.api.auth_deps import get_current_user
from app.infra.models import ProductORM, ProductStatus, ProductImageORM
from app.schemas.products import ProductOut, ProductImageOut  # ajuste se seus schemas tiverem nomes diferentes

from app.infra.storage_s3 import (
    upload_image_bytes,
    delete_object_best_effort,
    normalize_image_content_type,
    presign_get_url,
)

router = APIRouter(dependencies=[Depends(get_current_user)])

ALLOWED_CT = {"image/jpeg", "image/png", "image/webp"}
MAX_IMAGES = 4
MAX_BYTES = 2 * 1024 * 1024  # 2MB por imagem


def normalize_plate(plate: str) -> str:
    return plate.strip().upper().replace("-", "").replace(" ", "")


def normalize_chassi(chassi: str) -> str:
    return chassi.strip().upper().replace(" ", "")


def _product_out_with_presigned(product: ProductORM) -> ProductOut:
    """
    Converte ORM -> ProductOut e troca image.url (KEY) por URL presignada
    SEM mutar o ORM.
    """
    out = ProductOut.model_validate(product, from_attributes=True)  # pydantic v2
    if out.images:
        fixed = []
        for img in out.images:
            u = (img.url or "").strip()
            if u and not (u.startswith("http://") or u.startswith("https://")):
                u = presign_get_url(u, expires_seconds=3600)
            fixed.append(ProductImageOut(**{**img.model_dump(), "url": u}))
        out.images = fixed
    return out


def _products_out_with_presigned(products: Sequence[ProductORM]) -> list[ProductOut]:
    return [_product_out_with_presigned(p) for p in products]


@router.get("/{product_id}", response_model=ProductOut)
def get_product(product_id: int, db: Session = DBSession):
    stmt = (
        select(ProductORM)
        .options(selectinload(ProductORM.images))
        .where(ProductORM.id == product_id)
    )
    product = db.execute(stmt).scalars().first()
    if not product:
        raise HTTPException(status_code=404, detail="Produto não encontrado.")
    return _product_out_with_presigned(product)


@router.put("/{product_id}", response_model=ProductOut)
async def update_product(
    product_id: int,
    db: Session = DBSession,

    brand: Optional[str] = Form(None),
    model: Optional[str] = Form(None),
    year: Optional[int] = Form(None),
    plate: Optional[str] = Form(None),
    chassi: Optional[str] = Form(None),
    km: Optional[int] = Form(None),
    color: Optional[str] = Form(None),
    cost_price: Optional[Decimal] = Form(None),
    sale_price: Optional[Decimal] = Form(None),
    status: Optional[str] = Form(None),

    purchase_seller_name: Optional[str] = Form(None),
    purchase_seller_phone: Optional[str] = Form(None),
    purchase_seller_cpf: Optional[str] = Form(None),
    purchase_seller_address: Optional[str] = Form(None),

    images: list[UploadFile] = File(default=[]),
    replace_images: bool = Form(False),
):
    stmt = (
        select(ProductORM)
        .options(selectinload(ProductORM.images))
        .where(ProductORM.id == product_id)
    )
    product = db.execute(stmt).scalars().first()
    if not product:
        raise HTTPException(status_code=404, detail="Produto não encontrado.")

    # placa
    if plate is not None:
        plate_norm = normalize_plate(plate) if plate else None
        if plate_norm:
            exists_plate = db.scalar(
                select(ProductORM.id).where(
                    ProductORM.plate == plate_norm,
                    ProductORM.id != product_id,
                )
            )
            if exists_plate:
                raise HTTPException(status_code=409, detail="Placa já cadastrada.")
        product.plate = plate_norm

    # chassi
    if chassi is not None:
        chassi_norm = normalize_chassi(chassi) if chassi else None
        if not chassi_norm:
            raise HTTPException(status_code=400, detail="Chassi não pode ser vazio.")
        exists_chassi = db.scalar(
            select(ProductORM.id).where(
                ProductORM.chassi == chassi_norm,
                ProductORM.id != product_id,
            )
        )
        if exists_chassi:
            raise HTTPException(status_code=409, detail="Chassi já cadastrado.")
        product.chassi = chassi_norm

    if brand is not None:
        product.brand = brand.strip()
    if model is not None:
        product.model = model.strip()
    if year is not None:
        product.year = year
    if km is not None:
        product.km = km
    if color is not None:
        product.color = color.strip()
    if cost_price is not None:
        product.cost_price = cost_price
    if sale_price is not None:
        product.sale_price = sale_price

    if purchase_seller_name is not None:
        product.purchase_seller_name = purchase_seller_name.strip() or None
    if purchase_seller_phone is not None:
        product.purchase_seller_phone = purchase_seller_phone.strip() or None
    if purchase_seller_cpf is not None:
        product.purchase_seller_cpf = purchase_seller_cpf.strip() or None
    if purchase_seller_address is not None:
        product.purchase_seller_address = purchase_seller_address.strip() or None

    if status is not None:
        try:
            product.status = ProductStatus(status)
        except Exception:
            raise HTTPException(status_code=400, detail="Status de produto inválido.")

    db.flush()

    if images:
        if len(images) > MAX_IMAGES:
            raise HTTPException(status_code=400, detail=f"Máximo de {MAX_IMAGES} imagens.")

        for img in images:
            ct = normalize_image_content_type(img.content_type)
            if ct not in ALLOWED_CT:
                raise HTTPException(
                    status_code=415,
                    detail=f"Tipo de arquivo não suportado: {img.content_type}. Use jpeg/png/webp.",
                )

        # IMPORTANTE: aqui product.images ainda tem KEYs, porque a gente nunca presignou no ORM
        if replace_images:
            old_images = list(product.images)
            for old in old_images:
                delete_object_best_effort(old.url)  # old.url é KEY
                db.delete(old)
            db.flush()

        start_pos = len(product.images) + 1
        for idx, img in enumerate(images, start=start_pos):
            data = await img.read()
            if not data:
                raise HTTPException(status_code=400, detail="Arquivo de imagem vazio.")
            if len(data) > MAX_BYTES:
                raise HTTPException(status_code=400, detail="Imagem excede 2MB.")

            key = upload_image_bytes(
                data=data,
                content_type=img.content_type or "",
                key_prefix=f"products/{product.id}",
            )

            db.add(ProductImageORM(
                product_id=product.id,
                url=key,        # salva KEY
                position=idx,
            ))

        db.flush()

    db.commit()

    stmt2 = (
        select(ProductORM)
        .options(selectinload(ProductORM.images))
        .where(ProductORM.id == product_id)
    )
    updated = db.execute(stmt2).scalars().one()
    return _product_out_with_presigned(updated)


@router.post("", response_model=ProductOut, status_code=201)
async def create_product(
    db: Session = DBSession,

    brand: str = Form(..., min_length=2, max_length=60),
    model: str = Form(..., min_length=1, max_length=80),
    year: int = Form(..., ge=1900, le=2100),
    plate: Optional[str] = Form(default=None),
    chassi: str = Form(..., min_length=5, max_length=30),
    km: Optional[int] = Form(default=None, ge=0),
    color: str = Form(..., min_length=1, max_length=30),
    cost_price: Decimal = Form(default=Decimal("0.00")),
    sale_price: Decimal = Form(default=Decimal("0.00")),
    status: Optional[str] = Form(default=None),

    purchase_seller_name: Optional[str] = Form(default=None),
    purchase_seller_phone: Optional[str] = Form(default=None),
    purchase_seller_cpf: Optional[str] = Form(default=None),
    purchase_seller_address: Optional[str] = Form(default=None),

    images: list[UploadFile] = File(..., description="Envie de 2 a 4 imagens"),
):
    if not (2 <= len(images) <= 4):
        raise HTTPException(status_code=422, detail="Envie entre 2 e 4 imagens.")

    for img in images:
        ct = normalize_image_content_type(img.content_type)
        if ct not in ALLOWED_CT:
            raise HTTPException(
                status_code=415,
                detail=f"Tipo de arquivo não suportado: {img.content_type}. Use jpeg/png/webp.",
            )

    plate_n = normalize_plate(plate) if plate else None
    chassi_n = normalize_chassi(chassi)

    exists_chassi = db.scalar(select(ProductORM.id).where(ProductORM.chassi == chassi_n))
    if exists_chassi:
        raise HTTPException(status_code=409, detail="Chassi já cadastrado.")

    if plate_n:
        exists_plate = db.scalar(select(ProductORM.id).where(ProductORM.plate == plate_n))
        if exists_plate:
            raise HTTPException(status_code=409, detail="Placa já cadastrada.")

    st = ProductStatus.IN_STOCK
    if status:
        try:
            st = ProductStatus(status)
        except Exception:
            raise HTTPException(status_code=400, detail="Status de produto inválido.")

    product = ProductORM(
        brand=brand.strip(),
        model=model.strip(),
        year=year,
        plate=plate_n,
        chassi=chassi_n,
        km=km,
        color=color.strip(),
        cost_price=cost_price,
        sale_price=sale_price,
        status=st,

        purchase_seller_name=(purchase_seller_name.strip() if purchase_seller_name else None),
        purchase_seller_phone=(purchase_seller_phone.strip() if purchase_seller_phone else None),
        purchase_seller_cpf=(purchase_seller_cpf.strip() if purchase_seller_cpf else None),
        purchase_seller_address=(purchase_seller_address.strip() if purchase_seller_address else None),
    )
    db.add(product)
    db.flush()

    uploaded_keys: list[str] = []
    try:
        for idx, img in enumerate(images, start=1):
            data = await img.read()
            if not data:
                raise HTTPException(status_code=400, detail="Arquivo de imagem vazio.")
            if len(data) > MAX_BYTES:
                raise HTTPException(status_code=400, detail="Imagem excede 2MB.")

            key = upload_image_bytes(
                data=data,
                content_type=img.content_type or "",
                key_prefix=f"products/{product.id}",
            )
            uploaded_keys.append(key)

            db.add(ProductImageORM(
                product_id=product.id,
                url=key,  # salva KEY
                position=idx,
            ))

        db.flush()

        stmt = (
            select(ProductORM)
            .options(selectinload(ProductORM.images))
            .where(ProductORM.id == product.id)
        )
        created = db.execute(stmt).scalars().one()

        db.commit()
        return _product_out_with_presigned(created)

    except HTTPException:
        db.rollback()
        for key in uploaded_keys:
            delete_object_best_effort(key)
        raise
    except Exception:
        db.rollback()
        for key in uploaded_keys:
            delete_object_best_effort(key)
        raise HTTPException(status_code=500, detail="Erro ao salvar produto/imagens.")


@router.get("", response_model=list[ProductOut])
def list_products(
    db: Session = DBSession,
    q: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    stmt = (
        select(ProductORM)
        .options(selectinload(ProductORM.images))
        .order_by(ProductORM.id.desc())
    )

    if status:
        try:
            st = ProductStatus(status)
        except Exception:
            raise HTTPException(status_code=400, detail="Status inválido.")
        stmt = stmt.where(ProductORM.status == st)

    if q:
        qn = q.strip()
        qp = normalize_plate(qn)
        qc = normalize_chassi(qn)

        stmt = stmt.where(
            (ProductORM.brand.ilike(f"%{qn}%")) |
            (ProductORM.model.ilike(f"%{qn}%")) |
            (ProductORM.plate.ilike(f"%{qp}%")) |
            (ProductORM.chassi.ilike(f"%{qc}%"))
        )

    stmt = stmt.limit(limit).offset(offset)
    products = db.execute(stmt).scalars().all()
    return _products_out_with_presigned(products)
