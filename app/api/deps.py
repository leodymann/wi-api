from fastapi import Depends
from sqlalchemy.orm import Session
from app.infra.db import get_db

DBSession = Depends(get_db)