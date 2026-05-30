from sqlalchemy import select, delete, insert

from sqlalchemy.exc import SQLAlchemyError

from src.backend.models import (Chunk, Document, ExtractedItem,
                                TransliterationDictionary)
from src.utils.logger import PipelineLogger
from src.extraction.transliteration import build_transliteration_map
from src.utils.db import AsyncSessionLocal

logger = PipelineLogger.get_logger(__name__)


async def build_transliteration(doc_id: int) -> None:
    logger.info(f"[TRANSLITERATE] [DOC] [START] doc_id={doc_id}")
    with AsyncSessionLocal() as db:
        try:
            stmt = select(Document).where(Document.id == doc_id).with_for_update()
            rslt = await db.execute(stmt)
            doc = rslt.scalar()
            if not doc:
                logger.error(f"[TRANSLITERATE] [DOC] [ERROR] Документ не найден doc_id={doc_id}")
                return

            if not (doc.term_conflicts_done and doc.abbr_conflicts_done):
                logger.warning(
                    f"[TRANSLITERATE] [DOC] [WARNING] Словарь не готов для doc_id={doc_id}"
                )
                return

            stmt = select(ExtractedItem.word, ExtractedItem.definition).where(
                Chunk.doc_id == doc_id,
                ExtractedItem.item_type == "abbr",
                ExtractedItem.is_final
            ).join(Chunk)
            rslt = await db.execute(stmt)
            rows = rslt.all()

            abbreviations = {word: definition for word, definition in rows}

            if not abbreviations:
                logger.info(
                    f"[TRANSLITERATE] [DOC] [EMPTY] Нет аббревиатур для транслитерации, doc_id={doc_id}"
                )
                doc.status = "completed"
                await db.commit()
                return

            translit_map = build_transliteration_map(list(abbreviations.keys()), 5)
            logger.info(
                f"[TRANSLITERATE] [DOC] [BUILD] Построено вариантов: {len(translit_map)} для doc_id={doc_id}"
            )

            stmt = delete(TransliterationDictionary).where(TransliterationDictionary.doc_id == doc_id)
            await db.execute(stmt)

            if translit_map:
                stmt = insert(TransliterationDictionary)
                insert_data = [
                    {"doc_id": doc_id, "ru_variant": ru, "abbr": abbr}
                    for ru, abbr in translit_map.items()
                ]
                await db.execute(stmt, insert_data)

            doc.status = "completed"
            db.commit()
            logger.info(f"[TRANSLITERATE] [DOC] [FINISH] Документ doc_id={doc_id} завершён")

        except SQLAlchemyError as e:
            logger.error(
                f"[TRANSLITERATE] [DOC] [ERROR] Ошибка БД doc_id={doc_id}: {e}",
                exc_info=True,
            )
            db.rollback()
            raise
        except Exception as e:
            logger.error(
                f"[TRANSLITERATE] [DOC] [ERROR] Неожиданная ошибка doc_id={doc_id}: {e}",
                exc_info=True,
            )
            db.rollback()
            raise
        finally:
            db.close()
