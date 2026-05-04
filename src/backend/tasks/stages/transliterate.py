from celery.utils.log import get_task_logger
from sqlalchemy.exc import SQLAlchemyError

from src.backend.models import (Chunk, Document, ExtractedItem,
                                TransliterationDictionary)
from src.extraction.transliteration import build_transliteration_map
from src.utils.db import SessionLocal

logger = get_task_logger(__name__)


def build_transliteration(doc_id: int) -> None:
    logger.info(f"[TRANSLITERATE] [DOC] [START] doc_id={doc_id}")
    db = SessionLocal()
    try:
        doc = db.query(Document).filter(Document.id == doc_id).with_for_update().first()
        if not doc:
            logger.error(f"[TRANSLITERATE] [DOC] [ERROR] Документ не найден doc_id={doc_id}")
            return

        if not (doc.term_conflicts_done and doc.abbr_conflicts_done):
            logger.warning(
                f"[TRANSLITERATE] [DOC] [WARNING] Словарь не готов для doc_id={doc_id}"
            )
            return

        rows = (
            db.query(ExtractedItem.word, ExtractedItem.definition)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc_id,
                ExtractedItem.item_type == "abbr",
                ExtractedItem.is_final,
            )
            .all()
        )
        abbreviations = {word: definition for word, definition in rows}

        if not abbreviations:
            logger.info(
                f"[TRANSLITERATE] [DOC] [EMPTY] Нет аббревиатур для транслитерации, doc_id={doc_id}"
            )
            doc.status = "completed"
            db.commit()
            return

        translit_map = build_transliteration_map(list(abbreviations.keys()), 5)
        logger.info(
            f"[TRANSLITERATE] [DOC] [BUILD] Построено вариантов: {len(translit_map)} для doc_id={doc_id}"
        )

        db.query(TransliterationDictionary).filter_by(doc_id=doc_id).delete()
        db.bulk_save_objects(
            [
                TransliterationDictionary(doc_id=doc_id, ru_variant=ru, abbr=abbr)
                for ru, abbr in translit_map.items()
            ]
        )

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
