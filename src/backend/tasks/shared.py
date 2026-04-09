import asyncio
import os
from uuid import uuid4

from collections import defaultdict
from typing import Literal, List

from celery import Celery
from celery.utils.log import get_task_logger

from src.extraction.transliteration import build_transliteration_map

from src.utils.db import SessionLocal, update_system_status
from src.backend.models import (Document,
                                Chunk,
                                ExtractedItem,
                                TransliterationDictionary,
                                GlobalDictionary)

from src.backend.tasks.stages.extract import extract_items
from src.backend.tasks.stages.define import define_items
from src.backend.tasks.stages.resolve import resolve_items

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_upsert

logger = get_task_logger(__name__)

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")

app = Celery("nlp_pipeline", broker=CELERY_BROKER_URL)

ItemType = Literal["term", "abbr"]

# ---------------------------------------------------------------------------
# Конфиг-хелперы
# ---------------------------------------------------------------------------

_SEARCH_STAGE = {"term": "finding_term", "abbr": "finding_abbr"}
_DEFINE_STAGE = {"term": "defining_term", "abbr": "defining_abbr"}
_RESOLVE_STAGE = {"term": "resolve_term", "abbr": "resolve_abbr"}
_DEFINE_PROMPT_KEY = {"term": "term", "abbr": "abbr"}

_SEARCH_DONE_FLAG = {"term": "term_search_done", "abbr": "abbr_search_done"}
_DEFS_DONE_FLAG = {"term": "term_defs_done", "abbr": "abbr_defs_done"}
_CONFLICTS_DONE_FLAG = {"term": "term_conflicts_done", "abbr": "abbr_conflicts_done"}


def _bulk_extract(doc_id: int, chunk_ids: list[int], item_type: ItemType) -> None:
    logger.info(f"[EXTRACT] Начат батч {item_type} для doc_id={doc_id}. Чанков в батче: {len(chunk_ids)}")
    db = SessionLocal()
    try:
        chunks = db.query(Chunk).filter(Chunk.id.in_(chunk_ids)).all()
        if not chunks:
            logger.warning(f"[EXTRACT] doc_id={doc_id}: Чанки не найдены в БД, пропускаем.")
            return

        asyncio.run(extract_items(chunks, item_type, doc_id))

        db.execute(
            update(Document)
            .where(Document.id == doc_id)
            .values(**{f"{item_type}_batches_done": Document.__table__.c[f"{item_type}_batches_done"] + 1})
        )
        db.commit()

        doc = db.query(Document).filter(Document.id == doc_id).first()
        batches_done = getattr(doc, f"{item_type}_batches_done")
        batches_total = getattr(doc, f"{item_type}_batches_total")

        logger.info(f"[EXTRACT] doc_id={doc_id} {item_type} | Батч завершен: {batches_done}/{batches_total * 2}")

        if batches_done >= batches_total * 2:
            search_done_flag = _SEARCH_DONE_FLAG[item_type]
            locked_doc = db.query(Document).filter(Document.id == doc_id).with_for_update().first()

            if getattr(locked_doc, search_done_flag) is False:
                setattr(locked_doc, search_done_flag, True)
                db.commit()
            else:
                db.rollback()

    except Exception as e:
        logger.error(f"[EXTRACT] Критическая ошибка doc_id={doc_id}, type={item_type}: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()


def _bulk_define(doc_id: int, item_ids: list[int], item_type: str) -> None:
    logger.info(f"[DEFINE] Запуск для doc_id={str(doc_id)[:5]}, тип={item_type}, ID: {item_ids}")

    with SessionLocal() as db:
        rows = (
            db.query(ExtractedItem.id, ExtractedItem.word, Chunk.text)
            .join(Chunk, ExtractedItem.chunk_id == Chunk.id)
            .filter(ExtractedItem.id.in_(item_ids))
            .all()
        )

        if not rows:
            logger.info(f"[DEFINE] Батч пуст или уже обработан.")
            return

        logger.info(f"[DEFINE] [LAUNCH] Запуск асинхронного процессора для {item_type}, {len(rows)} элементов.")
        asyncio.run(define_items(doc_id, rows, item_type))

def _bulk_resolve(item_type: ItemType) -> None:
    logger.info(f"[RESOLVE-GLOBAL] Запуск сборки словаря для типа: {item_type}")
    update_system_status(f"build_{item_type}", "processing")
    db = SessionLocal()
    try:
        items = (
            db.query(ExtractedItem.word, ExtractedItem.definition)
            .filter(ExtractedItem.item_type == item_type)
            .filter(ExtractedItem.definition.isnot(None))
            .all()
        )

        grouped = defaultdict(set)
        for row in items:
            grouped[row.word].add(row.definition.strip())

        conflicts = {w: list(defs) for w, defs in grouped.items() if len(defs) > 1}
        ready_map = {w: list(defs)[0] for w, defs in grouped.items() if len(defs) == 1}

        logger.info(f"[RESOLVE-GLOBAL] Всего {len(grouped)} уникальных {item_type}. Конфликтов: {len(conflicts)}")

        if conflicts:
            resolved_map = asyncio.run(resolve_items(conflicts, item_type))
            ready_map.update(resolved_map)

        if ready_map:
            logger.info(f"[RESOLVE-GLOBAL] Синхронизация {len(ready_map)} записей с GlobalDictionary.")

            for word in sorted(ready_map.keys()):
                definition = ready_map[word]

                stmt = pg_upsert(GlobalDictionary).values(
                    id=uuid4(),
                    word=word,
                    item_type=item_type,
                    definition=definition
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['word'],
                    set_={
                        'definition': definition,
                        'item_type': item_type
                    }
                )
                db.execute(stmt)
            db.commit()

        db.query(ExtractedItem).filter(
            ExtractedItem.item_type == item_type,
            ExtractedItem.is_final == False
        ).update({"is_final": True}, synchronize_session=False)
        db.commit()

        logger.info(f"[RESOLVE-GLOBAL] Глобальный словарь ({item_type}) успешно обновлен.")
        update_system_status(f"build_{item_type}", "ready")

        if item_type == "abbr":
            logger.info(f"[RESOLVE-GLOBAL] Запускаю построение транслитерационного словаря.")
            abbreviations = list(ready_map.keys())
            _bulk_transliteration(abbreviations, 6)

    except Exception as e:
        logger.error(f"[RESOLVE-GLOBAL] Критическая ошибка: {e}", exc_info=True)
        update_system_status(f"build_{item_type}", "error", error=str(e))
        db.rollback()
        raise
    finally:
        db.close()


def _bulk_transliteration(abbreviations: List[str], max_length: int = 6) -> None:
    logger.info(f"[TRANSLITERATE] Запуск для всей базы данных")
    db = SessionLocal()
    try:
        doc = db.query(Document).with_for_update().first()

        if not abbreviations:
            logger.info(f"[TRANSLITERATE] В словаре нет аббревиатур для транслитерации. Финализация словаря.")
            doc.status = "completed"
            db.commit()
            return

        logger.info(f"[TRANSLITERATE] Построение вариантов для {len(abbreviations)} аббревиатур.")
        translit_map = build_transliteration_map(abbreviations, max_length)
        logger.info(f"[TRANSLITERATE] Построено {len(translit_map)} записей.")

        db.query(TransliterationDictionary).delete()
        db.bulk_save_objects([
            TransliterationDictionary(ru_variant=ru, abbr=abbr)
            for ru, abbr in translit_map.items()
        ])

        db.commit()
        logger.info(f"[TRANSLITERATE] Словарь полностью обработан.")

    except Exception as e:
        logger.error(f"[TRANSLITERATE] Ошибка при построении транслитерации словаря: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()