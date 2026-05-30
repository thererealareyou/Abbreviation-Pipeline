import httpx
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from config import config
from src.utils.db import get_db
from src.utils.logger import PipelineLogger

router = APIRouter(prefix="/system")
logger = PipelineLogger.get_logger(__name__)


@router.get("/health")
async def check_health(db: AsyncSession = Depends(get_db)):
    """
    Проверяет доступность LLM-сервера и базы данных.
    Логирует ошибки при недоступности компонентов.
    """
    llm_url = config.LLM_HEALTH_URL
    db_status = "ok"
    llm_status = "ok"

    try:
        await db.execute(text("SELECT 1"))
    except Exception as e:
        logger.error(f"[SYSTEM] [HEALTH] Ошибка подключения к БД: {e}", exc_info=True)
        db_status = "error"

    try:
        timeout = httpx.Timeout(connect=2.0, read=2.0, write=2.0, pool=2.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(llm_url)
        if response.status_code != 200:
            llm_status = "busy"
            logger.warning(
                f"[SYSTEM] [HEALTH] LLM вернул статус {response.status_code}"
            )
    except httpx.TimeoutException:
        llm_status = "unreachable"
        logger.error(
            f"[SYSTEM] [HEALTH] Таймаут при подключении к LLM ({llm_url})",
            exc_info=True,
        )
    except httpx.RequestError as e:
        llm_status = "unreachable"
        logger.error(
            f"[SYSTEM] [HEALTH] Ошибка подключения к LLM ({llm_url}): {e}",
            exc_info=True,
        )

    overall = "ok" if (db_status == "ok" and llm_status == "ok") else "error"
    if overall != "ok":
        logger.warning(
            f"[SYSTEM] [HEALTH] Сервис нездоров: db={db_status}, llm={llm_status}",
            exc_info=True,
        )
    else:
        logger.info(f"[SYSTEM] [HEALTH] Сервис здоров!")
    return {"status": overall, "db": db_status, "llm": llm_status}
