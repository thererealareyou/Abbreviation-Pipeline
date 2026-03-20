import os
import time
import random
import yaml
import httpx
import json

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import text
from pydantic import BaseModel
from typing import List

from src.backend.db import SessionLocal, init_db
from src.backend.models import Document, Chunk, ExtractedItem, TransliterationEntry
from src.backend.tasks import bulk_extract_terms_batch, bulk_extract_abbrs_batch


class UnicodeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(content, ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------------------
# Конфиг и приложение
# ---------------------------------------------------------------------------

with open("config/settings.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

app = FastAPI(title="Автоматический извлекатель доменных аббревиатур (АИДА)")
init_db()

CHUNK_BATCH = 100

# ---------------------------------------------------------------------------
# Схемы
# ---------------------------------------------------------------------------

class TextsRequest(BaseModel):
    document_id: str
    texts: List[str]


class ResetRequest(BaseModel):
    code: str


# ---------------------------------------------------------------------------
# Reset-хранилище
# ---------------------------------------------------------------------------

security_store: dict = {
    "reset_code": None,
    "expires_at": 0,
}


# ---------------------------------------------------------------------------
# Эндпоинты
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Global"])
async def check_health():
    """
    Проверяет доступность LLM-сервера и базы данных.

    Returns:
        status: 'ok' если оба сервиса доступны, 'error' если один из них недоступен.
        db: статус подключения к базе данных ('ok' / 'error').
        llm: статус LLM-сервера ('ok' / 'busy' / 'unreachable').
    """
    llm_url = os.getenv("LLM_API_URL")

    try:
        db = SessionLocal()
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


@app.post("/extract", tags=["Pipeline"])
def start_extraction(payload: TextsRequest):
    """
    Принимает массив текстовых чанков и запускает пайплайн извлечения.

    Если документ с таким document_id уже существует и находится в обработке,
    возвращает 409. Если документ уже обработан — перезапускает пайплайн с нуля.

    Args:
        payload.document_id: уникальный идентификатор документа.
        payload.texts: массив текстовых чанков (предложений).

    Returns:
        document_id, статус постановки в очередь и сообщение.
    """
    if not payload.texts:
        raise HTTPException(status_code=400, detail="Массив текстов не может быть пустым.")

    db = SessionLocal()
    try:
        doc = db.query(Document).filter_by(filename=payload.document_id).first()

        if doc:
            if doc.status == "processing":
                raise HTTPException(
                    status_code=409,
                    detail="Документ уже находится в обработке.",
                )

            doc.status = "processing"
            doc.term_search_done = False
            doc.abbr_search_done = False
            doc.term_defs_done = False
            doc.abbr_defs_done = False
            doc.term_conflicts_done = False
            doc.abbr_conflicts_done = False
            doc.final_dictionary = None
            doc.finding_abbr_chunks = 0
            doc.finding_term_chunks = 0
            doc.defining_abbrs = 0
            doc.defining_terms = 0
            doc.total_abbr_conflicts = 0
            doc.total_term_conflicts = 0
            doc.term_batches_total = 0
            doc.term_batches_done = 0
            doc.abbr_batches_total = 0
            doc.abbr_batches_done = 0
            db.query(Chunk).filter_by(doc_id=doc.id).delete()
        else:
            doc = Document(filename=payload.document_id, status="processing")
            db.add(doc)

        db.flush()
        chunks_to_insert = [Chunk(doc_id=doc.id, text=t) for t in payload.texts]
        db.bulk_save_objects(chunks_to_insert, return_defaults=True)

        total_batches = (len(payload.texts) + CHUNK_BATCH - 1) // CHUNK_BATCH
        doc.term_batches_total = total_batches
        doc.abbr_batches_total = total_batches
        db.commit()

        chunk_ids = [c.id for c in db.query(Chunk.id).filter_by(doc_id=doc.id).all()]
        for i in range(0, len(chunk_ids), CHUNK_BATCH):
            batch = chunk_ids[i:i + CHUNK_BATCH]
            bulk_extract_terms_batch.delay(doc.id, batch)
            bulk_extract_abbrs_batch.delay(doc.id, batch)

        return UnicodeJSONResponse({
            "document_id": payload.document_id,
            "status": "Task submitted to queue",
            "message": f"Пайплайн запущен. Чанков: {len(payload.texts)}, батчей: {total_batches}.",
        })
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")
    finally:
        db.close()


@app.get("/status/{document_id}", tags=["Status"])
def get_status(document_id: str):
    """
    Возвращает детальный статус обработки документа.

    Включает прогресс по каждому этапу пайплайна:
    - finding: поиск аббревиатур и терминов в чанках.
    - defining: поиск определений для найденных сущностей.
    - conflicts: разрешение конфликтующих определений.

    Args:
        document_id: идентификатор документа.

    Returns:
        Полный статус документа с прогрессом по всем этапам.
    """
    db = SessionLocal()
    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            raise HTTPException(status_code=404, detail="Документ не найден.")

        total_chunks = db.query(Chunk).filter_by(doc_id=doc.id).count()
        total_found_abbrs = (db.query(ExtractedItem).join(Chunk)
                             .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "abbr").count())
        total_found_terms = (db.query(ExtractedItem).join(Chunk)
                             .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "term").count())
        total_defined_abbrs = (db.query(ExtractedItem).join(Chunk)
                               .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "abbr",
                                       ExtractedItem.definition.isnot(None)).count())
        total_defined_terms = (db.query(ExtractedItem).join(Chunk)
                               .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "term",
                                       ExtractedItem.definition.isnot(None)).count())
        unique_defined_abbrs = (db.query(ExtractedItem.word).join(Chunk)
                                .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "abbr",
                                        ExtractedItem.definition.isnot(None)).distinct().count())
        unique_defined_terms = (db.query(ExtractedItem.word).join(Chunk)
                                .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "term",
                                        ExtractedItem.definition.isnot(None)).distinct().count())
        resolved_abbrs = (db.query(ExtractedItem.word).join(Chunk)
                          .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "abbr",
                                  ExtractedItem.is_final == True).distinct().count())
        resolved_terms = (db.query(ExtractedItem.word).join(Chunk)
                          .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "term",
                                  ExtractedItem.is_final == True).distinct().count())

        return UnicodeJSONResponse({
            "document_id": document_id,
            "status": doc.status,
            "stages": {
                "term_search": doc.term_search_done,
                "abbr_search": doc.abbr_search_done,
                "term_definitions": doc.term_defs_done,
                "abbr_definitions": doc.abbr_defs_done,
                "term_conflicts": doc.term_conflicts_done,
                "abbr_conflicts": doc.abbr_conflicts_done,
            },
            "finding": {
                "total_chunks": total_chunks,
                "abbr_chunks_processed": doc.finding_abbr_chunks,
                "term_chunks_processed": doc.finding_term_chunks,
                "found_abbrs": total_found_abbrs,
                "found_terms": total_found_terms,
            },
            "defining": {
                "abbrs_total": total_found_abbrs,
                "terms_total": total_found_terms,
                "abbrs_processed": doc.defining_abbrs,
                "terms_processed": doc.defining_terms,
                "defined_abbrs": total_defined_abbrs,
                "defined_terms": total_defined_terms,
            },
            "conflicts": {
                "abbr_unique_words": unique_defined_abbrs,
                "term_unique_words": unique_defined_terms,
                "abbr_processed": resolved_abbrs,
                "term_processed": resolved_terms,
                "abbr_actual_conflicts": doc.total_abbr_conflicts,
                "term_actual_conflicts": doc.total_term_conflicts,
            },
        })
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статуса: {str(e)}")
    finally:
        db.close()


@app.get("/stats", tags=["Status"])
def get_global_statistics():
    """
    Возвращает агрегированную статистику по всей базе данных.

    Полезно для мониторинга общей нагрузки на систему:
    количество документов, чанков и извлечённых сущностей.

    Returns:
        Статистика по документам, чанкам и извлечённым сущностям.
    """
    db = SessionLocal()
    try:
        total_docs = db.query(Document).count()
        completed_docs = db.query(Document).filter_by(status="completed").count()
        total_chunks = db.query(Chunk).count()
        total_items = db.query(ExtractedItem).count()
        abbr_count = db.query(ExtractedItem).filter_by(item_type="abbr").count()
        term_count = db.query(ExtractedItem).filter_by(item_type="term").count()
        defined_items = (db.query(ExtractedItem)
                         .filter(ExtractedItem.definition.isnot(None)).count())

        return UnicodeJSONResponse({
            "documents": {
                "total": total_docs,
                "completed": completed_docs,
                "in_progress": total_docs - completed_docs,
            },
            "chunks": {
                "total": total_chunks,
            },
            "extracted_entities": {
                "total_found": total_items,
                "abbreviations": abbr_count,
                "terms": term_count,
                "with_definitions": defined_items,
            },
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статистики: {str(e)}")
    finally:
        db.close()


@app.get("/result/{document_id}", tags=["Pipeline"])
def get_result(
    document_id: str,
    target: str,
    limit: int = 100,
    offset: int = 0,
):
    """
    Возвращает итоговый словарь или транслитерационную таблицу для документа.

    Документ должен находиться в статусе 'completed'.
    Поддерживает пагинацию через параметры limit и offset.

    Args:
        document_id: идентификатор документа.
        target: тип возвращаемых данных — 'abbr', 'term' или 'transliteration'.
        limit: максимальное количество записей (по умолчанию 100).
        offset: смещение для пагинации (по умолчанию 0).

    Returns:
        Словарь {слово: определение} или {ru_вариант: аббревиатура} для транслитерации.
    """
    if target not in ("abbr", "term", "transliteration"):
        raise HTTPException(
            status_code=400,
            detail="Параметр target должен быть 'abbr', 'term' или 'transliteration'.",
        )
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="limit должен быть от 1 до 1000.")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset не может быть отрицательным.")

    db = SessionLocal()
    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            raise HTTPException(status_code=404, detail="Документ не найден.")
        if doc.status != "completed":
            raise HTTPException(
                status_code=425,
                detail=f"Документ ещё не готов (статус: {doc.status}).",
            )

        if target in ("abbr", "term"):
            data = dict(
                db.query(ExtractedItem.word, ExtractedItem.definition)
                .join(Chunk)
                .filter(
                    Chunk.doc_id == doc.id,
                    ExtractedItem.item_type == target,
                    ExtractedItem.is_final == True,
                )
                .order_by(ExtractedItem.word)
                .offset(offset).limit(limit)
                .all()
            )
        else:
            data = dict(
                db.query(TransliterationEntry.ru_variant, TransliterationEntry.abbr)
                .filter_by(doc_id=doc.id)
                .order_by(TransliterationEntry.ru_variant)
                .offset(offset).limit(limit)
                .all()
            )

        return UnicodeJSONResponse({
            "document_id": document_id,
            "target": target,
            "offset": offset,
            "limit": limit,
            "count": len(data),
            "data": data,
        })
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении результата: {str(e)}")
    finally:
        db.close()


@app.get("/documents", tags=["Status"])
def list_documents(limit: int = 50, offset: int = 0):
    """
    Возвращает список всех документов в базе данных.

    Отсортирован по дате создания (сначала новые).
    Поддерживает пагинацию.

    Args:
        limit: максимальное количество документов (по умолчанию 50).
        offset: смещение для пагинации (по умолчанию 0).

    Returns:
        Список документов с id, именем, статусом и датой создания.
    """
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit должен быть от 1 до 500.")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset не может быть отрицательным.")

    db = SessionLocal()
    try:
        docs = (db.query(Document)
                .order_by(Document.created_at.desc())
                .offset(offset).limit(limit).all())
        return UnicodeJSONResponse({
            "total": db.query(Document).count(),
            "offset": offset,
            "limit": limit,
            "documents": [
                {
                    "id": d.id,
                    "filename": d.filename,
                    "status": d.status,
                    "created_at": str(d.created_at),
                }
                for d in docs
            ],
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении списка документов: {str(e)}")
    finally:
        db.close()


@app.delete("/document/{document_id}", tags=["Pipeline"])
def delete_document(document_id: str):
    """
    Удаляет документ и все связанные с ним данные.

    Каскадно удаляет чанки, извлечённые сущности и транслитерационные записи.
    Нельзя удалить документ в статусе 'processing'.

    Args:
        document_id: идентификатор документа.

    Returns:
        Подтверждение удаления.
    """
    db = SessionLocal()
    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            raise HTTPException(status_code=404, detail="Документ не найден.")
        if doc.status == "processing":
            raise HTTPException(
                status_code=409,
                detail="Нельзя удалить документ в процессе обработки. Дождитесь завершения.",
            )
        db.delete(doc)
        db.commit()
        return UnicodeJSONResponse({
            "status": "success",
            "message": f"Документ '{document_id}' и все связанные данные удалены.",
        })
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка при удалении документа: {str(e)}")
    finally:
        db.close()


@app.get("/danger/generate-reset-code", tags=["Reset"])
def generate_reset_code():
    """
    Генерирует одноразовый 6-значный код для подтверждения полной очистки БД.

    Код действителен 60 секунд. Используется совместно с DELETE /danger/clear-database.

    Returns:
        Сгенерированный код и сообщение о времени действия.
    """
    code = str(random.randint(100000, 999999))
    security_store["reset_code"] = code
    security_store["expires_at"] = time.time() + 60
    return {
        "message": "Код сброса сгенерирован. Действует 60 секунд.",
        "code": code,
    }


@app.delete("/danger/clear-database", tags=["Reset"])
def clear_database(payload: ResetRequest):
    """
    Полностью очищает все таблицы в базе данных.

    Требует предварительного получения кода через GET /danger/generate-reset-code.
    Код действителен 60 секунд. Операция необратима.

    Args:
        payload.code: одноразовый код подтверждения.

    Returns:
        Подтверждение очистки.
    """
    if not security_store["reset_code"] or time.time() > security_store["expires_at"]:
        raise HTTPException(status_code=400, detail="Код истёк или не был сгенерирован.")
    if payload.code != security_store["reset_code"]:
        raise HTTPException(status_code=403, detail="Неверный код подтверждения.")

    db = SessionLocal()
    try:
        db.query(TransliterationEntry).delete()
        db.query(ExtractedItem).delete()
        db.query(Chunk).delete()
        db.query(Document).delete()
        db.commit()

        security_store["reset_code"] = None
        security_store["expires_at"] = 0

        return {"status": "success", "message": "База данных полностью очищена."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка при очистке базы данных: {str(e)}")
    finally:
        db.close()
