from celery.utils.log import get_task_logger

from src.backend.models import (Chunk, Document, ExtractedItem,
                                TransliterationDictionary)
from src.extraction.transliteration import build_transliteration_map
from src.utils.db import SessionLocal

logger = get_task_logger(__name__)


def build_transliteration(doc_id: int) -> None:
    db = SessionLocal()
    try:
        doc = db.query(Document).filter(Document.id == doc_id).with_for_update().first()
        if not doc:
            logger.error(f"[build_transliteration] Документ {doc_id} не найден")
            return

        if not (doc.term_conflicts_done and doc.abbr_conflicts_done):
            logger.warning(
                f"[build_transliteration] Словарь ещё не готов для doc_id={doc_id}"
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
                f"[build_transliteration] Нет аббревиатур для транслитерации, doc_id={doc_id}"
            )
            doc.status = "completed"
            db.commit()
            return

        translit_map = build_transliteration_map(list(abbreviations.keys()), 5)
        logger.info(
            f"[build_transliteration] Построено {len(translit_map)} вариантов для doc_id={doc_id}"
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

    except Exception as e:
        logger.error(f"[build_transliteration] Критическая ошибка doc_id={doc_id}: {e}")
        db.rollback()
        raise
    finally:
        db.close()
