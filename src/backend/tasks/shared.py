import asyncio
from collections import defaultdict
from typing import List, Literal, Union
from uuid import uuid4, UUID

from sqlalchemy import func, update, select, delete, insert
from sqlalchemy.dialects.postgresql import insert as pg_upsert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from src.utils.logger import PipelineLogger

from src.backend.models import (Chunk, Document, ExtractedItem,
                                GlobalDictionary, TransliterationDictionary)
from src.backend.tasks.stages.define import define_items
from src.backend.tasks.stages.extract import extract_items
from src.backend.tasks.stages.resolve import resolve_items
from src.extraction.transliteration import build_transliteration_map
from src.utils.db import AsyncSessionLocal, update_system_status

logger = PipelineLogger.get_logger(__name__)

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


async def _bulk_extract(doc_id: int, chunk_ids: list[int], target_item_type: ItemType) -> None:
    logger.info(
        f"[EXTRACT] [BATCH] [START] doc_id={doc_id}, type={target_item_type}, chunks_in_batch={len(chunk_ids)}"
    )
    async with AsyncSessionLocal() as db:
        try:
            stmt = select(Chunk).where(Chunk.id.in_(chunk_ids))
            rslt = await db.execute(stmt)
            chunks = rslt.scalars().all()
            if not chunks:
                logger.warning(
                    f"[EXTRACT] [BATCH] [EMPTY] doc_id={doc_id}: чанки не найдены, пропуск"
                )
                return

            await extract_items(db, chunks, target_item_type, doc_id)
            await db.commit()

        except SQLAlchemyError as e:
            logger.error(
                f"[EXTRACT] [BATCH] [ERROR] Ошибка БД doc_id={doc_id}, type={target_item_type}: {e}",
                exc_info=True,
            )
            await db.rollback()
            raise
        except Exception as e:
            logger.error(
                f"[EXTRACT] [BATCH] [ERROR] Неожиданная ошибка doc_id={doc_id}, type={target_item_type}: {e}",
                exc_info=True,
            )
            await db.rollback()
            raise
        finally:
            await db.close()


async def _bulk_define(doc_id: int, item_ids: list[int], item_type: str) -> None:
    logger.info(
        f"[DEFINE] [BATCH] [START] doc_id={doc_id}, type={item_type}, items_in_batch={len(item_ids)}"
    )

    async with AsyncSessionLocal() as db:
        try:
            stmt = (
                select(ExtractedItem.id, ExtractedItem.word, Chunk.text)
                .join(Chunk, ExtractedItem.chunk_id == Chunk.id)
                .where(ExtractedItem.id.in_(item_ids), ExtractedItem.is_final.is_(False))
                .with_for_update(skip_locked=True)
            )
            rslt = await db.execute(stmt)
            rows = rslt.all()

            if not rows:
                logger.info(f"[DEFINE] [BATCH] [EMPTY] doc_id={doc_id}: нет элементов для обработки")
                return

            actual_locked_ids = [r.id for r in rows]
            logger.debug(f"[DEFINE] [BATCH] [LOCKED] Заблокировано элементов: {len(actual_locked_ids)}")

            await define_items(db, doc_id, rows, item_type)

            stmt = (
                update(ExtractedItem)
                .where(ExtractedItem.id.in_(actual_locked_ids))
                .values(is_final=True))

            await db.execute(stmt)

            await db.commit()
            logger.info(
                f"[DEFINE] [BATCH] [FINISH] doc_id={doc_id}, type={item_type}, обработано {len(actual_locked_ids)} элементов"
            )

        except SQLAlchemyError as e:
            logger.error(
                f"[DEFINE] [BATCH] [ERROR] Ошибка БД doc_id={doc_id}, type={item_type}: {e}",
                exc_info=True,
            )
            await db.rollback()
            raise
        except Exception as e:
            logger.error(
                f"[DEFINE] [BATCH] [ERROR] Неожиданная ошибка doc_id={doc_id}, type={item_type}: {e}",
                exc_info=True,
            )
            await db.rollback()
            raise


async def _bulk_resolve(doc_id: int, target_item_type: ItemType) -> None:
    logger.info(f"[RESOLVE] [GLOBAL] [START] Запуск сборки глобального словаря для {target_item_type}")
    await update_system_status(f"build_{target_item_type}", "processing")

    async with AsyncSessionLocal() as db:
        try:
            stmt = (
                select(ExtractedItem.word, ExtractedItem.definition)
                .where(
                    ExtractedItem.item_type == target_item_type,
                    ExtractedItem.definition.isnot(None),
                    ExtractedItem.definition != ""
                )
            )
            rslt = await db.execute(stmt)
            items = rslt.all()

            grouped = defaultdict(set)
            for row in items:
                grouped[row.word].add(row.definition.strip())

            conflicts = {w: list(defs) for w, defs in grouped.items() if len(defs) > 1}
            ready_map = {w: list(defs)[0] for w, defs in grouped.items() if len(defs) == 1}

            logger.info(
                f"[RESOLVE] [GLOBAL] [STATS] Всего уникальных {target_item_type}: {len(grouped)}, конфликтов: {len(conflicts)}"
            )

            if conflicts:
                resolved_map = await resolve_items(db, conflicts, target_item_type)
                ready_map.update(resolved_map)
                logger.info(f"[RESOLVE] [GLOBAL] [RESOLVED] Конфликтов разрешено: {len(conflicts)}")

            if ready_map:
                logger.info(
                    f"[RESOLVE] [GLOBAL] [SYNC] Синхронизация {len(ready_map)} записей с GlobalDictionary"
                )

                records = [
                    {
                        "id": uuid4(),
                        "word": word,
                        "item_type": target_item_type,
                        "definition": def_val
                    }
                    for word, def_val in ready_map.items()
                ]

                insert_stmt = pg_upsert(GlobalDictionary).values(records)

                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=["word"],
                    set_={
                        "definition": insert_stmt.excluded.definition,
                        "item_type": insert_stmt.excluded.item_type
                    },
                )

                await db.execute(upsert_stmt)
                await db.commit()

            update_stmt = (
                update(ExtractedItem)
                .values(is_final=True)
                .where(
                    ExtractedItem.item_type == target_item_type,
                    ExtractedItem.is_final == False
                )
            )
            await db.execute(update_stmt)
            await db.commit()

            logger.info(f"[RESOLVE] [GLOBAL] [FINISH] Словарь {target_item_type} готов")
            await update_system_status(f"build_{target_item_type}", "ready")

        except SQLAlchemyError as e:
            logger.error(
                f"[RESOLVE] [GLOBAL] [ERROR] Ошибка БД при сборке словаря {target_item_type}: {e}",
                exc_info=True,
            )
            await update_system_status(f"build_{target_item_type}", "error", error=str(e))
            await db.rollback()
            raise
        except Exception as e:
            logger.error(
                f"[RESOLVE] [GLOBAL] [ERROR] Неожиданная ошибка при сборке словаря {target_item_type}: {e}",
                exc_info=True,
            )
            await update_system_status(f"build_{target_item_type}", "error", error=str(e))
            await db.rollback()
            raise
        finally:
            await db.close()


async def _bulk_transliterate(doc_id: Union[UUID, int, str]) -> None:
    is_global = str(doc_id) == "0"

    logger.info(
        f"[TRANSLITERATE] [START] Запуск транслитерации. Режим: {'ГЛОБАЛЬНЫЙ' if is_global else f'doc_id={doc_id}'}"
    )

    async with AsyncSessionLocal() as db:
        try:
            if not is_global:
                if isinstance(doc_id, str):
                    doc_id = UUID(doc_id)

                stmt = select(Document).where(Document.id == doc_id).with_for_update()
                rslt = await db.execute(stmt)
                doc = rslt.scalar_one_or_none()

                if not doc:
                    logger.error(f"[TRANSLITERATE] [ERROR] Документ не найден doc_id={doc_id}")
                    return

            stmt = select(ExtractedItem.word, ExtractedItem.definition).where(
                ExtractedItem.item_type == "abbr",
                ExtractedItem.is_final == True
            ).join(Chunk)

            if not is_global:
                stmt = stmt.where(Chunk.doc_id == doc_id)

            rslt = await db.execute(stmt)
            rows = rslt.all()

            if not rows:
                logger.info(
                    f"[TRANSLITERATE] [EMPTY] Нет аббревиатур для транслитерации ({'global' if is_global else f'doc_id={doc_id}'})")
                await db.commit()
                return

            abbreviations = {word: definition for word, definition in rows}

            translit_map = build_transliteration_map(list(abbreviations.keys()), max_abbr_len=5)
            logger.info(f"[TRANSLITERATE] [BUILD] Построено вариантов: {len(translit_map)}")

            delete_stmt = delete(TransliterationDictionary)
            if not is_global:
                delete_stmt = delete_stmt.where(TransliterationDictionary.doc_id == doc_id)

            await db.execute(delete_stmt)

            if translit_map:
                db_doc_id = None if is_global else doc_id

                insert_data = [
                    {"doc_id": db_doc_id, "ru_variant": ru, "abbr": abbr}
                    for ru, abbr in translit_map.items()
                ]
                await db.execute(insert(TransliterationDictionary), insert_data)

            await db.commit()
            logger.info(f"[TRANSLITERATE] [FINISH] Транслитерационный словарь успешно сохранен")

        except SQLAlchemyError as e:
            logger.error(f"[TRANSLITERATE] [GLOBAL] [ERROR] Ошибка БД: {e}", exc_info=True)
            await db.rollback()
            raise
        except Exception as e:
            logger.error(f"[TRANSLITERATE] [GLOBAL] [ERROR] Неожиданная ошибка: {e}", exc_info=True)
            await db.rollback()
            raise
        finally:
            await db.close()
