import asyncio
import yaml
import aiohttp

from typing import Literal
from sqlalchemy import update

from celery.utils.log import get_task_logger

from src.extraction.model_client import get_llm_client, parse_llm_definition_response
from src.extraction.regex_detector import verify_expansion_term, verify_expansion_abbr
from src.backend.models import Document, ExtractedItem
from src.utils.db import SessionLocal


logger = get_task_logger(__name__)

_DEFINE_STAGE = {"term": "defining_term", "abbr": "defining_abbr"}
ItemType = Literal["term", "abbr"]

with open("config/prompts.yaml", "r", encoding="utf-8") as f:
    prompts = yaml.safe_load(f)


def _sync_save_definitions_and_progress(results: dict[int, str], doc_id: int, item_type: str, count: int):
    """Синхронная запись в БД: сохраняет сами определения и обновляет прогресс."""
    with SessionLocal() as db:
        try:
            for item_id, text in results.items():
                db.query(ExtractedItem).filter(ExtractedItem.id == item_id).update(
                    {"definition": text},
                    synchronize_session=False
                )

            db.execute(
                update(Document)
                .where(Document.id == doc_id)
                .values(**{f"defining_{item_type}s": getattr(Document, f"defining_{item_type}s") + count})
            )
            db.commit()
            logger.info(f"[DEFINE] doc_id={doc_id}: Успешно сохранено {len(results)} определений ({item_type}).")
        except Exception as e:
            db.rollback()
            logger.error(f"[DEFINE] Ошибка БД при сохранении для doc_id={doc_id}: {e}")


async def define_items(
        doc_id: int,
        items_with_context: list[tuple[int, str, str]],
        item_type: str,
) -> None:
    stage = "defining_term" if item_type == "term" else "defining_abbr"
    instructions = prompts["llm"][stage]["instructions"]
    model = get_llm_client()

    sem = asyncio.Semaphore(5)

    async def process_one(session, item_id: int, word: str, chunk_text: str):
        async with sem:
            try:
                prompt = instructions.format(chunk_text=chunk_text, item=word)
                raw = await model.generate_async(session, prompt, stage=stage)

                logger.info(f"[DEFINE] [LLM] Отправляю запрос | {item_type} | {word} | {chunk_text[:25]}.")

                if not raw:
                    return item_id, ""

                definition = parse_llm_definition_response(raw)

                if item_type == "abbr":
                    is_valid = verify_expansion_abbr(word, definition, chunk_text)
                else:
                    is_valid = verify_expansion_term(word, definition, chunk_text)
                if is_valid:
                    return item_id, definition
                return item_id, ""
            except Exception as e:
                logger.error(f"[DEFINE] Ошибка LLM для word='{word}': {e}")
                return item_id, "Определение не найдено"

    async with aiohttp.ClientSession() as session:
        tasks = [
            process_one(session, item_id, word, chunk_text)
            for item_id, word, chunk_text in items_with_context
        ]
        raw_results = await asyncio.gather(*tasks)

    results = {res[0]: res[1] for res in raw_results if res is not None}

    if results:
        logger.info(f"[LLM_END] Получено определений: {len(results)}.")
        await asyncio.to_thread(
            _sync_save_definitions_and_progress,
            results,
            doc_id,
            item_type,
            len(items_with_context)
        )