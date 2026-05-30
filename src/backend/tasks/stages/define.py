import asyncio
from typing import Literal

from sqlalchemy.ext.asyncio import AsyncSession

import aiohttp
import yaml
from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError
from src.utils.logger import PipelineLogger

from src.backend.models import Document, ExtractedItem
from src.extraction.model_client import (get_llm_client,
                                         parse_llm_definition_response)
from src.extraction.regex_detector import (verify_expansion_abbr,
                                           verify_expansion_term)
from src.utils.db import AsyncSessionLocal

logger = PipelineLogger.get_logger(__name__)

_DEFINE_STAGE = {"term": "defining_term", "abbr": "defining_abbr"}
ItemType = Literal["term", "abbr"]

with open("config/prompts.yaml", "r", encoding="utf-8") as f:
    prompts = yaml.safe_load(f)


async def define_items(
        db: AsyncSession,
        doc_id: int,
        items_with_context: list[tuple[int, str, str]],
        item_type: str,
) -> None:
    stage = "defining_term" if item_type == "term" else "defining_abbr"
    instructions = prompts["llm"][stage]["instructions"]
    model = get_llm_client()

    logger.info(
        f"[DEFINE] [START] doc_id={doc_id}, type={item_type}, items={len(items_with_context)}"
    )

    sem = asyncio.Semaphore(5)

    async def process_one(session, item_id: int, word: str, chunk_text: str):
        async with sem:
            try:
                prompt = instructions.format(chunk_text=chunk_text, item=word)
                raw = await model.generate_async(session, prompt, stage=stage)

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

            except aiohttp.ClientError as e:
                logger.error(
                    f"[DEFINE] [LLM] [ERROR] Сетевая ошибка для '{word}' (id={item_id}): {e}"
                )
                return item_id, ""
            except Exception as e:
                logger.error(
                    f"[DEFINE] [LLM] [ERROR] Ошибка LLM для '{word}' (id={item_id}): {e}",
                    exc_info=True,
                )
                return item_id, ""

    async with aiohttp.ClientSession() as session:
        tasks = [
            process_one(session, item_id, word, chunk_text)
            for item_id, word, chunk_text in items_with_context
        ]
        raw_results = await asyncio.gather(*tasks)

    results = {res[0]: res[1] for res in raw_results if res is not None}

    if results:
        logger.info(f"[DEFINE] [LLM] [END] Получено определений: {len(results)}")
        
        for item_id, text in results.items():
            stmt = (
                update(ExtractedItem)
                .where(ExtractedItem.id == item_id)
                .values(definition=text)
            )
            await db.execute(stmt)

        doc_counter_field = f"defining_{item_type}s"
        await db.execute(
            update(Document)
            .where(Document.id == doc_id)
            .values({doc_counter_field: getattr(Document, doc_counter_field) + len(items_with_context)})
        )

        await db.flush()
    else:
        logger.info(
            f"[DEFINE] [FINISH] [EMPTY] doc_id={doc_id}: не получено ни одного определения"
        )
