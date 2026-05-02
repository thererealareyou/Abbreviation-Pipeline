from dotenv import load_dotenv
from sqlalchemy import create_engine, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from config import config
from src.backend.models import SystemState

load_dotenv()

DATABASE_URL = config.DATABASE_URL

engine = create_engine(
    DATABASE_URL,
    json_serializer=lambda obj: __import__("json").dumps(obj, ensure_ascii=False),
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def update_system_status(key: str, status: str, error: str = None):
    with SessionLocal() as db:
        stmt = insert(SystemState).values(
            key=key, value=status, error_message=error, updated_at=func.now()
        )

        stmt = stmt.on_conflict_do_update(
            index_elements=["key"],
            set_={"value": status, "error_message": error, "updated_at": func.now()},
        )

        db.execute(stmt)
        db.commit()
