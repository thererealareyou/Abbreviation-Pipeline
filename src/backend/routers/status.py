from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, select

from src.backend.models import Document, Chunk, ExtractedItem, SystemState, GlobalDictionary
from src.utils.db import get_db


router = APIRouter(
    prefix="/status"
)


@router.get("/documents/detailed/{document_id}")
def get_doc_status(document_id: str, db: Session = Depends(get_db)):
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
    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            raise HTTPException(status_code=404, detail="Документ не найден.")

        total_chunks = db.query(Chunk).filter_by(doc_id=doc.id).count()

        total_found_abbrs = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "abbr")
            .count()
        )

        total_found_terms = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(Chunk.doc_id == doc.id, ExtractedItem.item_type == "term")
            .count()
        )

        total_defined_abbrs = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc.id,
                ExtractedItem.item_type == "abbr",
                ExtractedItem.is_final == True,
                ExtractedItem.definition.isnot(None),
                ExtractedItem.definition != ""
            )
            .count()
        )

        total_defined_terms = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc.id,
                ExtractedItem.item_type == "term",
                ExtractedItem.is_final == True,
                ExtractedItem.definition.isnot(None),
                ExtractedItem.definition != ""
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
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении статуса: {str(e)}")

@router.get("/documents/stats")
def get_documents_statistics(db: Session = Depends(get_db)):
    """
    Возвращает расширенную статистику: документы, сырые находки (с учетом уникальности)
    и состояние итогового глобального словаря.
    """
    try:
        total_docs = db.query(Document).count()
        completed_docs = db.query(Document).filter_by(status="completed").count()

        raw_stats = db.query(
            ExtractedItem.item_type,
            func.count(ExtractedItem.id).label("total"),
            func.count(func.distinct(ExtractedItem.word)).label("unique")
        ).group_by(ExtractedItem.item_type).all()

        raw_data = {
            r.item_type: {"total": r.total, "unique": r.unique}
            for r in raw_stats
        }

        global_stats = db.query(
            GlobalDictionary.item_type,
            func.count(GlobalDictionary.id)
        ).group_by(GlobalDictionary.item_type).all()

        global_map = {item_type: count for item_type, count in global_stats}

        build_states = db.execute(
            select(SystemState).where(SystemState.key.like("build_%"))
        ).scalars().all()
        is_syncing = any(s.value == "processing" for s in build_states)

        return {
            "system_status": {
                "is_dictionary_building": is_syncing,
                "last_sync_states": {s.key: s.value for s in build_states}
            },
            "documents": {
                "total": total_docs,
                "completed": completed_docs,
                "in_progress": total_docs - completed_docs,
            },
            "raw_extractions": {
                "total_mentions": sum(d["total"] for d in raw_data.values()),
                "abbrs": {
                    "mentions": raw_data.get("abbr", {}).get("total", 0),
                    "unique": raw_data.get("abbr", {}).get("unique", 0),
                },
                "terms": {
                    "mentions": raw_data.get("term", {}).get("total", 0),
                    "unique": raw_data.get("term", {}).get("unique", 0),
                },
            },
            "global_dictionary": {
                "total_entities": sum(global_map.values()),
                "final_abbrs": global_map.get("abbr", 0),
                "final_terms": global_map.get("term", 0),
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка статистики: {str(e)}")


@router.get("/documents/list")
def list_documents(limit: int = 50, offset: int = 0, db: Session = Depends(get_db)):
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

    try:
        docs = (db.query(Document)
                .order_by(Document.created_at.desc())
                .offset(offset).limit(limit).all())
        return {
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
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении списка документов: {str(e)}")
