from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from pathlib import Path
import os

from app.infra.db import engine
from app.infra.models import Base

from app.api.routers.clients import router as clients_router
from app.api.routers.products import router as products_router
from app.api.routers.sales import router as sales_router
from app.api.routers.promissories import router as promissories_router
from app.api.routers.installments import router as installments_router
from app.api.routers.users import router as users_router
from app.api.routers.finance import router as finance_router
from app.api.routers.auth import router as auth_router
from app.api.routers.test import router as test
from app.api.routers.health import router as health_router
from dotenv import load_dotenv
from app.api.routers.health import router as health_router
load_dotenv()
# make uploads path configurable (can be replaced with S3 in prod)
#UPLOAD_ROOT = Path(os.environ.get("UPLOAD_ROOT", "uploads"))

# allowed origins can be provided as a comma-separated env var
_env_origins = os.environ.get("FRONTEND_URLS") or os.environ.get("ALLOWED_ORIGINS")
if _env_origins:
    ALLOW_ORIGINS_LIST = [o.strip() for o in _env_origins.split(",") if o.strip()]
else:
    ALLOW_ORIGINS_LIST = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://tauri.localhost",
]

app = FastAPI(title="Moto Store API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS_LIST,  # mantém lista fixa p/ produção
    allow_origin_regex=os.getenv("CORS_ORIGIN_REGEX"),  # regex opcional
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.middleware("http")
async def log_preflight(request: Request, call_next):
    if request.method == "OPTIONS":
        print("[PRE-FLIGHT] path =", request.url.path)
        print("[PRE-FLIGHT] origin =", request.headers.get("origin"))
        print("[PRE-FLIGHT] acrm =", request.headers.get("access-control-request-method"))
        print("[PRE-FLIGHT] acrh =", request.headers.get("access-control-request-headers"))
    return await call_next(request)
print("[CORS] allow_origins =", ALLOW_ORIGINS_LIST)

@app.on_event("startup")
def _startup() -> None:
    print("[startup] creating tables...")
    Base.metadata.create_all(bind=engine)
    print("[startup] tables created/checked")
    #UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)

#app.mount("/static", StaticFiles(directory=str(UPLOAD_ROOT)), name="static")



app.include_router(clients_router, prefix="/clients", tags=["clients"])
app.include_router(products_router, prefix="/products", tags=["products"])
app.include_router(sales_router, prefix="/sales", tags=["sales"])
app.include_router(promissories_router, prefix="/promissories", tags=["promissories"])
app.include_router(installments_router, prefix="/installments", tags=["installments"])
app.include_router(users_router, prefix="/users", tags=["users"])
app.include_router(finance_router, prefix="/finance", tags=["finance"])
app.include_router(auth_router, prefix="/auth", tags=["auth"])
#app.include_router(test, prefix="/test", tags=["test"])
app.include_router(health_router, tags=["health"])





