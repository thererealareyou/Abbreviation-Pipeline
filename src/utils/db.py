from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from config import config
from typing import AsyncGenerator
from src.backend.models import SystemState
from src.utils.logger import PipelineLogger

logger = PipelineLogger.get_logger(__name__)

engine = create_async_engine(
    config.DATABASE_URL,
    json_serializer=lambda obj: __import__("json").dumps(obj, ensure_ascii=False),
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as db:
        try:
            yield db
        finally:
            await db.close()


async def update_system_status(key: str, status: str, error: str = None):
    logger.info(f"[SYSTEM] [STATE] [UPDATE] Установка состояния: key={key}, value={status}, error={error}")
    try:
        async with AsyncSessionLocal() as db:
            stmt = insert(SystemState).values(
                key=key, value=status, error_message=error, updated_at=func.now()
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["key"],
                set_={"value": status, "error_message": error, "updated_at": func.now()},
            )
            await db.execute(stmt)
            await db.commit()

            logger.info(f"[SYSTEM] [STATE] [SUCCESS] Состояние {key} обновлено успешно")
    except SQLAlchemyError as e:
        logger.error(
            f"[SYSTEM] [STATE] [ERROR] Ошибка БД при обновлении состояния {key}: {e}",
            exc_info=True,
        )
        raise
