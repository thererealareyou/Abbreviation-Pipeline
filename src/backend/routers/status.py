from arq import ArqRedis
from psycopg.errors import InvalidTextRepresentation
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.models import (Chunk, Document, ExtractedItem,
                                GlobalDictionary, SystemState)
from src.utils.db import get_db
from src.utils.logger import PipelineLogger
from src.utils.arq_utils import get_arq_pool

router = APIRouter(prefix="/status")
logger = PipelineLogger.get_logger(__name__)


@router.get("/documents/detailed/{document_id}")
async def get_doc_status(document_id: UUID,
                         db: AsyncSession = Depends(get_db),
                         redis: ArqRedis = Depends(get_arq_pool)
                         ):
    logger.info(f"[STATUS] [DOC] [REQUEST] Запрос статуса документа: {document_id}")
    try:
        stmt = select(Document).where(Document.id == document_id)
        rslt = await db.execute(stmt)
        doc = rslt.scalar_one_or_none()

        if not doc:
            logger.warning(f"[STATUS] [DOC] [LOOKUP] Документ с id={document_id} не найден")
            raise HTTPException(status_code=404, detail="Документ не найден.")

        status = doc.status
        is_done = status == "completed"

        stmt = select(func.count()).select_from(Chunk).where(Chunk.doc_id == doc.id)
        total_chunks = (await db.execute(stmt)).scalar_one_or_none() or 0

        stmt_abbr = select(func.count()).select_from(ExtractedItem).join(Chunk).where(
            and_(Chunk.doc_id == doc.id, ExtractedItem.item_type == "abbr"))
        total_found_abbrs = (await db.execute(stmt_abbr)).scalar_one_or_none() or 0

        stmt_term = select(func.count()).select_from(ExtractedItem).join(Chunk).where(
            and_(Chunk.doc_id == doc.id, ExtractedItem.item_type == "term"))
        total_found_terms = (await db.execute(stmt_term)).scalar_one_or_none() or 0

        if is_done:
            abbrs_extracted_chunks = total_chunks
            terms_extracted_chunks = total_chunks

            abbrs_definition_processed = total_found_abbrs
            terms_definition_processed = total_found_terms
        else:
            abbrs_extracted_chunks = int(
                await redis.get(f"doc:{document_id}:abbrs_extraction_processed") or doc.finding_abbr_chunks or 0)
            terms_extracted_chunks = int(
                await redis.get(f"doc:{document_id}:terms_extraction_processed") or doc.finding_term_chunks or 0)

            abbrs_definition_processed = int(
                await redis.get(f"doc:{document_id}:abbrs_definition_processed") or doc.defining_abbrs or 0)
            terms_definition_processed = int(
                await redis.get(f"doc:{document_id}:terms_definition_processed") or doc.defining_terms or 0)

        stmt_def_abbr = select(func.count()).select_from(ExtractedItem).join(Chunk).where(
            and_(Chunk.doc_id == doc.id, ExtractedItem.item_type == "abbr", ExtractedItem.is_final,
                 ExtractedItem.definition.isnot(None), ExtractedItem.definition != "")
        )
        total_defined_abbrs = (await db.execute(stmt_def_abbr)).scalar_one_or_none() or 0

        stmt_def_term = select(func.count()).select_from(ExtractedItem).join(Chunk).where(
            and_(Chunk.doc_id == doc.id, ExtractedItem.item_type == "term", ExtractedItem.is_final,
                 ExtractedItem.definition.isnot(None), ExtractedItem.definition != "")
        )
        total_defined_terms = (await db.execute(stmt_def_term)).scalar_one_or_none() or 0

        stages = {
            "abbrs_extract": is_done or status in ["defining", "resolving", "transliterating"],
            "terms_extract": is_done or status in ["defining", "resolving", "transliterating"],
            "abbrs_define": is_done or status in ["resolving", "transliterating"],
            "terms_define": is_done or status in ["resolving", "transliterating"]
        }

        return {
            "document_id": document_id,
            "status": status,
            "stages": stages,
            "extracting": {
                "chunks_total": total_chunks,
                "abbrs_extraction_processed": abbrs_extracted_chunks,
                "terms_extraction_processed": terms_extracted_chunks,
                "abbrs_extracted": total_found_abbrs,
                "terms_extracted": total_found_terms,
            },
            "defining": {
                "abbrs_total": total_found_abbrs,
                "terms_total": total_found_terms,
                "abbrs_definition_processed": abbrs_definition_processed,
                "terms_definition_processed": terms_definition_processed,
                "abbrs_defined": total_defined_abbrs,
                "terms_defined": total_defined_terms,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[STATUS] [DOC] [ERROR] Неожиданная ошибка: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/documents/stats")
async def get_documents_statistics(db: AsyncSession = Depends(get_db)):
    """
    Возвращает расширенную статистику: документы, сырые находки (с учетом уникальности)
    и состояние итогового глобального словаря.
    """
    logger.info("[STATUS] [STATS] [REQUEST] Запрос расширенной статистики")
    try:
        stmt = select(func.count()).select_from(Document)
        rslt = await db.execute(stmt)
        total_docs = rslt.scalar()

        stmt = select(func.count()).where(Document.status == "completed").select_from(Document)
        rslt = await db.execute(stmt)
        completed_docs = rslt.scalar()

        logger.info(
            f"[STATUS] [STATS] Документы: total={total_docs}, completed={completed_docs}"
        )

        stmt = (
            select(
                ExtractedItem.item_type,
                func.count(ExtractedItem.id).label("total"),
                func.count(func.distinct(ExtractedItem.word)).label("unique"),
            )
            .group_by(ExtractedItem.item_type)
        )
        rslt = await db.execute(stmt)
        raw_stats = rslt.all()

        raw_data = {
            r.item_type: {"total": r.total, "unique": r.unique} for r in raw_stats
        }
        logger.info(
            f"[STATUS] [STATS] Сырые находки: abbr={raw_data.get('abbr')}, term={raw_data.get('term')}"
        )

        stmt = (
            select(GlobalDictionary.item_type, func.count(GlobalDictionary.id))
            .group_by(GlobalDictionary.item_type)
        )
        rslt = await db.execute(stmt)
        global_stats = rslt.all()

        global_map = {item_type: count for item_type, count in global_stats}
        logger.info(
            f"[STATUS] [STATS] Глобальный словарь: {global_map}"
        )

        stmt = select(SystemState).where(SystemState.key.like("build_%"))
        rslt = await db.execute(stmt)
        build_states = rslt.scalars().all()

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
async def list_documents(limit: int = 50, offset: int = 0, db: AsyncSession = Depends(get_db)):
    """
    Возвращает список всех документов в базе данных.

    Отсортирован по дате создания (сначала новые).
    Поддерживает пагинацию.

    Args:
        limit (int): максимальное количество документов (по умолчанию 50).
        offset (int): смещение для пагинации (по умолчанию 0).

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
        stmt = select(Document).order_by(Document.created_at.desc()).offset(offset).limit(limit)
        rslt = await db.execute(stmt)
        docs = rslt.scalars().all()

        stmt = select(func.count()).select_from(Document)
        rslt = await db.execute(stmt)
        total = rslt.scalar()

        logger.info(
            f"[STATUS] [LIST] [RESULT] Найдено документов: {len(docs)}, всего: {total}"
        )
        return {
            "total": total,
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
