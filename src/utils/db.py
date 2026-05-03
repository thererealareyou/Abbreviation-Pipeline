from sqlalchemy import create_engine, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from config import config
from src.backend.models import SystemState
from src.utils.logger import PipelineLogger

logger = PipelineLogger.get_logger(__name__)
engine = create_engine(
    config.DATABASE_URL,
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
    logger.info(f"[SYSTEM] [STATE] [UPDATE] Установка состояния: key={key}, value={status}, error={error}")
    try:
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
            logger.info(f"[SYSTEM] [STATE] [SUCCESS] Состояние {key} обновлено успешно")
    except SQLAlchemyError as e:
        logger.error(
            f"[SYSTEM] [STATE] [ERROR] Ошибка БД при обновлении состояния {key}: {e}",
            exc_info=True,
        )
        raise
