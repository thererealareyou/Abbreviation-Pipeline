import json
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.backend.models import Base

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/nlp_db")

def _json_serializer(obj):
    return json.dumps(obj, ensure_ascii=False)

engine = create_engine(
    DATABASE_URL,
    json_serializer=_json_serializer,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Создаёт таблицы, если они ещё не существуют."""
    print("Проверяем и создаём таблицы, если их нет...")
    Base.metadata.create_all(bind=engine)
    print("Готово!")