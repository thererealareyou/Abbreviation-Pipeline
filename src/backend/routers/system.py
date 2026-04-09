import httpx

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.utils.db import get_db

from config import config


router = APIRouter(
    prefix="/system"
)


@router.get("/health")
async def check_health(db: Session = Depends(get_db)):
    """
    Проверяет доступность LLM-сервера и базы данных.

    Returns:
        status: 'ok' если оба сервиса доступны, 'error' если один из них недоступен.
        db: статус подключения к базе данных ('ok' / 'error').
        llm: статус LLM-сервера ('ok' / 'busy' / 'unreachable').
    """
    llm_url = config.LLM_API_URL

    try:
        db.execute(text("SELECT 1"))
        db.close()
        db_status = "ok"
    except Exception:
        db_status = "error"

    try:
        timeout = httpx.Timeout(connect=2.0, read=2.0, write=2.0, pool=2.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(f"{llm_url}/health")
        llm_status = "ok" if response.status_code == 200 else "busy"
    except httpx.TimeoutException:
        llm_status = "busy"
    except httpx.RequestError:
        llm_status = "unreachable"

    overall = "ok" if db_status == "ok" else "error"
    return {"status": overall, "db": db_status, "llm": llm_status}
