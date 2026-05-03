from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.backend.models import (Chunk, Document, ExtractedItem,
                                GlobalDictionary, SystemState)
from src.utils.db import get_db
from src.utils.logger import PipelineLogger

router = APIRouter(prefix="/status")
logger = PipelineLogger.get_logger(__name__)


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
    logger.info(
        f"[STATUS] [DOC] [REQUEST] Запрос статуса документа: {document_id}"
    )
    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            logger.warning(
                f"[STATUS] [DOC] [LOOKUP] Документ с filename={document_id} не найден"
            )
            raise HTTPException(status_code=404, detail="Документ не найден.")

        logger.info(
            f"[STATUS] [DOC] [LOOKUP] Документ найден: id={doc.id}, status={doc.status}"
        )

        total_chunks = db.query(Chunk).filter_by(doc_id=doc.id).count()
        logger.debug(f"[CHUNK] [COUNT] Всего чанков для doc_id={doc.id}: {total_chunks}")

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

        logger.info(
            f"[EXTRACT] [COUNT] Найдено сущностей: abbr={total_found_abbrs}, term={total_found_terms}"
        )

        total_defined_abbrs = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc.id,
                ExtractedItem.item_type == "abbr",
                ExtractedItem.is_final,
                ExtractedItem.definition.isnot(None),
                ExtractedItem.definition != "",
            )
            .count()
        )

        total_defined_terms = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc.id,
                ExtractedItem.item_type == "term",
                ExtractedItem.is_final,
                ExtractedItem.definition.isnot(None),
                ExtractedItem.definition != "",
            )
            .count()
        )

        logger.info(
            f"[DEFINE] [COUNT] Определений: abbr={total_defined_abbrs}, term={total_defined_terms}"
        )

        logger.info(f"[STATUS] [DOC] [RESULT] Статус документа {document_id} сформирован успешно")

        return {
            "document_id": document_id,
            "status": doc.status,
            "stages": {
                "abbrs_extract": doc.abbr_search_done,
                "terms_extract": doc.term_search_done,
                "abbrs_define": doc.abbr_defs_done,
                "terms_define": doc.term_defs_done,
            },
            "extracting": {
                "chunks_total": total_chunks,
                "abbrs_extraction_processed": doc.finding_abbr_chunks,
                "terms_extraction_processed": doc.finding_term_chunks,
                "abbrs_extracted": total_found_abbrs,
                "terms_extracted": total_found_terms,
            },
            "defining": {
                "abbrs_total": total_found_abbrs,
                "terms_total": total_found_terms,
                "abbrs_definition_processed": doc.defining_abbrs,
                "terms_definition_processed": doc.defining_terms,
                "abbrs_defined": total_defined_abbrs,
                "terms_defined": total_defined_terms,
            },
        }

    except HTTPException:
        raise

    except SQLAlchemyError as e:
        logger.error(
            f"[STATUS] [DOC] [ERROR] Ошибка БД при получении статуса документа {document_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Ошибка базы данных при получении статуса")

    except Exception as e:
        logger.error(
            f"[STATUS] [DOC] [ERROR] Неожиданная ошибка при получении статуса документа {document_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/documents/stats")
def get_documents_statistics(db: Session = Depends(get_db)):
    """
    Возвращает расширенную статистику: документы, сырые находки (с учетом уникальности)
    и состояние итогового глобального словаря.
    """
    logger.info("[STATUS] [STATS] [REQUEST] Запрос расширенной статистики")
    try:
        total_docs = db.query(Document).count()
        completed_docs = db.query(Document).filter_by(status="completed").count()
        logger.info(
            f"[STATUS] [STATS] Документы: total={total_docs}, completed={completed_docs}"
        )

        raw_stats = (
            db.query(
                ExtractedItem.item_type,
                func.count(ExtractedItem.id).label("total"),
                func.count(func.distinct(ExtractedItem.word)).label("unique"),
            )
            .group_by(ExtractedItem.item_type)
            .all()
        )

        raw_data = {
            r.item_type: {"total": r.total, "unique": r.unique} for r in raw_stats
        }
        logger.info(
            f"[STATUS] [STATS] Сырые находки: abbr={raw_data.get('abbr')}, term={raw_data.get('term')}"
        )

        global_stats = (
            db.query(GlobalDictionary.item_type, func.count(GlobalDictionary.id))
            .group_by(GlobalDictionary.item_type)
            .all()
        )

        global_map = {item_type: count for item_type, count in global_stats}
        logger.info(
            f"[STATUS] [STATS] Глобальный словарь: {global_map}"
        )

        build_states = (
            db.execute(select(SystemState).where(SystemState.key.like("build_%")))
            .scalars()
            .all()
        )
        is_syncing = any(s.value == "processing" for s in build_states)
        logger.info(
            f"[STATUS] [STATS] Состояние синхронизации: is_building={is_syncing}, states={ {s.key: s.value for s in build_states} }"
        )
        logger.info("[STATUS] [STATS] [RESULT] Расширенная статистика успешно собрана")
        return {
            "system_status": {
                "is_dictionary_building": is_syncing,
                "last_sync_states": {s.key: s.value for s in build_states},
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
            },
        }
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            f"[STATUS] [STATS] [ERROR] Ошибка БД при сборе статистики: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Ошибка базы данных при запросе статистики")
    except Exception as e:
        logger.error(
            f"[STATUS] [STATS] [ERROR] Неожиданная ошибка: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

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
    logger.info(
        f"[STATUS] [LIST] [REQUEST] Запрос списка документов: limit={limit}, offset={offset}"
    )
    if limit < 1 or limit > 500:
        logger.warning(
            f"[STATUS] [LIST] [VALIDATION] Недопустимый limit={limit}"
        )
        raise HTTPException(status_code=400, detail="limit должен быть от 1 до 500.")
    if offset < 0:
        logger.warning(
            f"[STATUS] [LIST] [VALIDATION] Недопустимый offset={offset}"
        )
        raise HTTPException(
            status_code=400, detail="offset не может быть отрицательным."
        )

    try:
        docs = (
            db.query(Document)
            .order_by(Document.created_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        total = db.query(Document).count()
        logger.info(
            f"[STATUS] [LIST] [RESULT] Найдено документов: {len(docs)}, всего: {total}"
        )
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
    except SQLAlchemyError as e:
        logger.error(
            f"[STATUS] [LIST] [ERROR] Ошибка БД при получении списка документов (limit={limit}, offset={offset}): {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=500, detail="Ошибка базы данных при получении списка документов"
        )
    except Exception as e:
        logger.error(
            f"[STATUS] [LIST] [ERROR] Неожиданная ошибка: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=500, detail="Внутренняя ошибка сервера"
        )
