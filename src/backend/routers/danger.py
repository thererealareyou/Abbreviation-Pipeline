import random
import time

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete
from sqlalchemy.orm import Session

from src.backend.models import Chunk, Document, ExtractedItem
from src.backend.schemas import ResetRequest
from src.utils.db import get_db
from src.utils.logger import PipelineLogger

router = APIRouter(prefix="/danger")
logger = PipelineLogger.get_logger(__name__)


security_store: dict = {
    "reset_code": None,
    "expires_at": 0,
}


@router.get("/generate-reset-code")
def generate_reset_code():
    """
    Генерирует одноразовый 6-значный код для подтверждения очистки БД документов.

    Код действителен 60 секунд. Используется совместно с ручкой /danger/clear-database.

    Returns:
        Сгенерированный код и сообщение о времени действия.
    """
    code = str(random.randint(100000, 999999))
    security_store["reset_code"] = code
    security_store["expires_at"] = time.time() + 60
    logger.info("[DANGER] [CODE] [REQUEST] Сгенерирован новый код сброса (действителен 60 сек)")
    return {
        "message": "Код сброса сгенерирован. Действует 60 секунд.",
        "code": code,
    }


@router.delete("/clear-database")
def clear_database(payload: ResetRequest, db: Session = Depends(get_db)):
    """
    Очищает базу данных документов, оставляет глобальный словарь и транслитерационную таблицу.

    Для предотвращения случайных нажатий нужен код из ручки danger/generate-reset-code.
    """
    logger.info("[DANGER] [CLEAR] [REQUEST] Запрос на очистку базы данных документов")
    if not security_store["reset_code"] or time.time() > security_store["expires_at"]:
        logger.warning("[DANGER] [CLEAR] [VALIDATION] Код недействителен или истёк")
        raise HTTPException(
            status_code=400, detail="Код истёк или не был сгенерирован."
        )

    if payload.code != security_store["reset_code"]:
        logger.warning("[DANGER] [CLEAR] [VALIDATION] Неверный код подтверждения")
        raise HTTPException(status_code=403, detail="Неверный код подтверждения.")

    try:
        db.execute(delete(ExtractedItem))
        db.execute(delete(Chunk))
        db.execute(delete(Document))
        db.commit()

        security_store["reset_code"] = None
        security_store["expires_at"] = 0
        logger.info("[DANGER] [CLEAR] [RESULT] База документов (Item, Chunk, Doc) очищена. Код сброшен.")
        return {"status": "success", "message": "База данных полностью очищена."}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка при очистке БД: {str(e)}")
