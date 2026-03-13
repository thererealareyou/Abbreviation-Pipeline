import time
import random
import yaml
import httpx
import json

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List

from src.utils.db import SessionLocal
from src.utils.models import Document, Chunk, ExtractedItem, TransliterationEntry
from src.utils.tasks import (
    bulk_extract_terms_batch,
    bulk_extract_abbrs_batch,
    resolve_term_conflicts,
    resolve_abbr_conflicts,
)

class UnicodeJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(content, ensure_ascii=False).encode("utf-8")

# ---------------------------------------------------------------------------
# Конфиг и приложение
# ---------------------------------------------------------------------------

with open("config/settings.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

app = FastAPI(title="Автоматический извлекатель доменных аббревиатур (АИДА)")

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
    """Проверяет доступность LLM-сервера."""
    llm_url = config["llm"]["api"]["url"]
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{llm_url}/health")
        if response.status_code == 200:
            return {"status": "ok", "llm_health": response.json()}
        return {"status": "error", "llm_health": f"HTTP {response.status_code}"}
    except httpx.RequestError:
        return {"status": "error", "detail": "LLM server is unreachable"}


@app.post("/extract", tags=["Pipeline"])
def start_extraction(payload: TextsRequest):
    """
    Принимает массив текстовых чанков и запускает пайплайн.
    Если документ с таким именем уже существует и находится в обработке — возвращает ошибку.
    """
    if not payload.texts:
        raise HTTPException(status_code=400, detail="Empty texts array")

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
            db.query(Chunk).filter_by(doc_id=doc.id).delete()
        else:
            doc = Document(filename=payload.document_id, status="processing")
            db.add(doc)

        db.flush()
        chunks_to_insert = [Chunk(doc_id=doc.id, text=t) for t in payload.texts]
        db.bulk_save_objects(chunks_to_insert, return_defaults=True)
        db.commit()

        chunk_ids = [c.id for c in db.query(Chunk.id).filter_by(doc_id=doc.id).all()]

        for i in range(0, len(chunk_ids), CHUNK_BATCH):
            batch = chunk_ids[i:i + CHUNK_BATCH]
            bulk_extract_terms_batch.delay(doc.id, batch)
            bulk_extract_abbrs_batch.delay(doc.id, batch)

        return {
                "document_id": payload.document_id,
                "status": "Task submitted to queue",
                "message": "Пайплайн запущен",
            }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        db.close()


@app.get("/status/{document_id}", tags=["Status"])
def get_status(document_id: str):
    """Возвращает статус обработки документа по его имени."""
    db = SessionLocal()
    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")

        total_chunks = db.query(Chunk).filter_by(doc_id=doc.id).count()

        total_items = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(Chunk.doc_id == doc.id)
            .count()
        )

        defined_items = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc.id,
                ExtractedItem.definition.isnot(None),
            )
            .count()
        )

        return {
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
            "chunks": {
                "total": total_chunks,
                "entities_found": total_items,
                "with_definitions": defined_items,
            },
        }
    finally:
        db.close()


@app.get("/stats", tags=["Status"])
def get_global_statistics():
    """Возвращает общую аналитику по всей базе данных."""
    db = SessionLocal()
    try:
        total_docs = db.query(Document).count()
        completed_docs = db.query(Document).filter_by(status="completed").count()
        total_chunks = db.query(Chunk).count()
        total_items = db.query(ExtractedItem).count()
        abbr_count = db.query(ExtractedItem).filter_by(item_type="abbr").count()
        term_count = db.query(ExtractedItem).filter_by(item_type="term").count()
        defined_items = (
            db.query(ExtractedItem)
            .filter(ExtractedItem.definition.isnot(None))
            .count()
        )

        return {
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
        }
    finally:
        db.close()


@app.post("/dictionary/build/{document_id}", tags=["Pipeline"])
def build_final_dictionary(document_id: str):
    """
    Запускает разрешение конфликтов и формирование итогового словаря.
    Вызывать только после завершения стадий поиска и определений.
    """
    db = SessionLocal()
    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")

        if not (doc.term_search_done and doc.abbr_search_done):
            raise HTTPException(
                status_code=400,
                detail="Сначала завершите стадии извлечения (Search).",
            )

        if not (doc.term_defs_done and doc.abbr_defs_done):
            raise HTTPException(
                status_code=400,
                detail="Сначала завершите стадии поиска определений (Definitions).",
            )

        if doc.term_conflicts_done and doc.abbr_conflicts_done:
            return {
                "status": "Already completed",
                "message": "Словарь уже сформирован и конфликты разрешены.",
            }

        doc.status = "resolving_conflicts"
        db.commit()

        if not doc.term_conflicts_done:
            resolve_term_conflicts.delay(doc.id)

        if not doc.abbr_conflicts_done:
            resolve_abbr_conflicts.delay(doc.id)

        return {
            "status": "Task submitted to queue",
            "message": "Разрешение конфликтов запущено в фоне.",
        }
    finally:
        db.close()

@app.get("/result/{document_id}", tags=["Pipeline"])
def get_result(
    document_id: str,
    target: str,
    limit: int = 100,
    offset: int = 0,
):
    if target not in ("abbr", "term", "transliteration"):
        raise HTTPException(status_code=400, detail="target должен быть 'abbr', 'term' или 'transliteration'")

    db = SessionLocal()
    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        if doc.status != "completed":
            raise HTTPException(status_code=425, detail=f"Документ ещё не готов (статус: {doc.status})")

        if target in ("abbr", "term"):
            data = dict(
                db.query(ExtractedItem.word, ExtractedItem.definition)
                .join(Chunk)
                .filter(
                    Chunk.doc_id == doc.id,
                    ExtractedItem.item_type == target,
                    ExtractedItem.is_final == True,
                )
                .offset(offset).limit(limit)
                .all()
            )
        else:  # transliteration
            data = dict(
                db.query(TransliterationEntry.ru_variant, TransliterationEntry.abbr)
                .filter_by(doc_id=doc.id)
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
    finally:
        db.close()



@app.get("/danger/generate-reset-code", tags=["Reset"])
def generate_reset_code():
    """Генерирует 6-значный код для подтверждения очистки БД. Действует 60 секунд."""
    code = str(random.randint(100000, 999999))
    security_store["reset_code"] = code
    security_store["expires_at"] = time.time() + 60
    return {"message": "Reset code generated. It will expire in 60 seconds.", "code": code}


@app.delete("/danger/clear-database", tags=["Reset"])
def clear_database(payload: ResetRequest):
    """Полностью очищает все таблицы в БД при совпадении кода."""
    if not security_store["reset_code"] or time.time() > security_store["expires_at"]:
        raise HTTPException(status_code=400, detail="Code expired or not generated")

    if payload.code != security_store["reset_code"]:
        raise HTTPException(status_code=403, detail="Invalid reset code")

    db = SessionLocal()
    try:
        db.query(ExtractedItem).delete()
        db.query(Chunk).delete()
        db.query(Document).delete()
        db.commit()

        security_store["reset_code"] = None
        security_store["expires_at"] = 0

        return {"status": "success", "message": "Database completely cleared"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        db.close()
