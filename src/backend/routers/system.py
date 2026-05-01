import logging
import httpx
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.utils.db import get_db
from config import config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/system")


@router.get("/health")
async def check_health(db: Session = Depends(get_db)):
    """
    Проверяет доступность LLM-сервера и базы данных.
    Логирует ошибки при недоступности компонентов.
    """
    llm_url = config.LLM_API_URL
    db_status = "ok"
    llm_status = "ok"

    try:
        db.execute(text("SELECT 1"))
    except Exception as e:
        logger.error(
            f"[SYSTEM] [HEALTH] Ошибка подключения к БД: {e}",
            exc_info=True
        )
        db_status = "error"

    try:
        timeout = httpx.Timeout(connect=2.0, read=2.0, write=2.0, pool=2.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(f"{llm_url}/health")
        if response.status_code != 200:
            llm_status = "busy"
            logger.warning(
                f"[SYSTEM] [HEALTH] LLM вернул статус {response.status_code}"
            )
    except httpx.TimeoutException:
        llm_status = "unreachable"
        logger.error(
            f"[SYSTEM] [HEALTH] Таймаут при подключении к LLM ({llm_url})",
            exc_info=True
        )
    except httpx.RequestError as e:
        llm_status = "unreachable"
        logger.error(
            f"[SYSTEM] [HEALTH] Ошибка подключения к LLM ({llm_url}): {e}",
            exc_info=True
        )

    overall = "ok" if (db_status == "ok") else "error"
    if overall != "ok":
        logger.warning(
            f"[SYSTEM] [HEALTH] Сервис нездоров: db={db_status}, llm={llm_status}"
        )

    return {"status": overall, "db": db_status, "llm": llm_status}