import asyncio
import re
from typing import Literal

import aiohttp
import yaml
from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError
from src.utils.logger import PipelineLogger

from config import config
from src.backend.models import Chunk, Document, ExtractedItem
from src.extraction.model_client import (get_llm_client,
                                         parse_llm_extraction_response)
from src.extraction.regex_detector import clean_abbr_list, clean_terms_list
from src.utils.db import AsyncSessionLocal

logger = PipelineLogger.get_logger(__name__)

ItemType = Literal["term", "abbr"]

BATCH_SIZE = config.BATCH_SIZE

with open("config/prompts.yaml", "r", encoding="utf-8") as f:
    prompts = yaml.safe_load(f)


async def extract_items(db, chunks: list[Chunk], item_type: ItemType, doc_id: int) -> None:
    """
    Этап экстракции: поиск терминов/аббревиатур в тексте чанков.
    На входе: список объектов Chunk из БД.
    """
    stage = "finding_term" if item_type == "term" else "finding_abbr"
    instructions = prompts["llm"][stage]["instructions"]
    model = get_llm_client()

    logger.info(
        f"[EXTRACT] [START] doc_id={doc_id}, type={item_type}, chunks={len(chunks)}"
    )

    sem = asyncio.Semaphore(5)

    async def process_one(session, chunk: Chunk):
        async with sem:
            try:
                text = chunk.text
                text = re.sub(r"[*~#]", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
                prompt = instructions.format(chunk_text=text)
                raw = await model.generate_async(session, prompt, stage=stage)

                if not raw:
                    return []

                found_words = parse_llm_extraction_response(raw)

                if item_type == "abbr":
                    found_words = clean_abbr_list(found_words, text)
                else:
                    found_words = clean_terms_list(found_words, text)

                return [
                    ExtractedItem(
                        chunk_id=chunk.id,
                        item_type=item_type,
                        word=word.strip(),
                        definition=None,
                        is_final=False,
                    )
                    for word in found_words
                    if word.strip()
                ]

            except aiohttp.ClientError as e:
                logger.error(
                    f"[EXTRACT] [LLM] [ERROR] Сетевая ошибка в чанке id={chunk.id}: {e}"
                )
                return []
            except Exception as e:
                logger.error(
                    f"[EXTRACT] [LLM] [ERROR] Ошибка в чанке id={chunk.id}: {e}",
                    exc_info=True,
                )
                return []

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[process_one(session, chunk) for chunk in chunks]
        )

        all_new_items = [item for sublist in results for item in sublist]

    try:
        if all_new_items or chunks:
            if all_new_items:
                db.add_all(all_new_items)
                await db.flush()

            field_name = f"finding_{item_type}_chunks"
            batch_field = f"{item_type}_batches_done"

            await db.execute(
                update(Document)
                .where(Document.id == doc_id)
                .values(
                    {
                        field_name: getattr(Document, field_name) + len(chunks),
                        batch_field: getattr(Document, batch_field) + 1,
                    }
                )
            )

            logger.info(
                f"[EXTRACT] [FINISH] doc_id={doc_id}: обработано чанков={len(chunks)}, найдено {item_type}={len(all_new_items)}"
            )

    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(
            f"[EXTRACT] [DB] [ERROR] Ошибка БД для doc_id={doc_id}: {e}",
            exc_info=True,
        )
        raise
    except Exception as e:
        await db.rollback()
        logger.error(
            f"[EXTRACT] [ERROR] Неожиданная ошибка для doc_id={doc_id}: {e}",
            exc_info=True,
        )
        raise
