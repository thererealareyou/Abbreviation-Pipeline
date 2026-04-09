import asyncio
import yaml
import aiohttp

from typing import Literal
from celery.utils.log import get_task_logger

from config import config
from sqlalchemy import update
from src.utils.db import SessionLocal
from src.backend.models import ExtractedItem, Chunk, Document
from src.extraction.model_client import get_llm_client
from src.extraction.model_client import parse_llm_extraction_response


logger = get_task_logger(__name__)

ItemType = Literal["term", "abbr"]

with open("config/prompts.yaml", "r", encoding="utf-8") as f:
    prompts = yaml.safe_load(f)


async def extract_items(chunks: list[Chunk], item_type: ItemType, doc_id: int) -> None:
    """
    Этап экстракции: поиск терминов/аббревиатур в тексте чанков.
    На входе: список объектов Chunk из БД.
    """
    stage = "finding_term" if item_type == "term" else "finding_abbr"
    instructions = prompts["llm"][stage]["instructions"]
    model = get_llm_client()

    sem = asyncio.Semaphore(5)

    async def process_one(session, chunk: Chunk):
        async with sem:
            try:
                prompt = instructions.format(chunk_text=chunk.text)
                raw = await model.generate_async(session, prompt, stage=stage)


                logger.info(f"[EXTRACT] [LLM] Отправляю запрос | {item_type} | {chunk.text[:25]}.")

                if not raw:
                    return []

                found_words = parse_llm_extraction_response(raw)

                return [
                    ExtractedItem(
                        chunk_id=chunk.id,
                        item_type=item_type,
                        word=word.strip(),
                        definition=None,
                        is_final=False
                    )
                    for word in found_words if word.strip()
                ]
            except Exception as e:
                logger.error(f"[EXTRACT] Ошибка в чанке id={chunk.id}: {e}")
                return []

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[process_one(session, chunk) for chunk in chunks])

        all_new_items = [item for sublist in results for item in sublist]

    if all_new_items or chunks:
        with SessionLocal() as db:
            try:
                if all_new_items:
                    db.add_all(all_new_items)
                    db.flush()

                field_name = f"finding_{item_type}_chunks"
                batch_field = f"{item_type}_batches_done"

                db.execute(
                    update(Document)
                    .where(Document.id == doc_id)
                    .values({
                        field_name: getattr(Document, field_name) + len(chunks),
                        batch_field: getattr(Document, batch_field) + 1
                    })
                )

                db.commit()

                BATCH_SIZE = config.BATCH_SIZE
                new_item_ids = [item.id for item in all_new_items]

                if item_type == "abbr":
                    from src.backend.tasks.public import bulk_define_abbrs
                    for i in range(0, len(new_item_ids), BATCH_SIZE):
                        batch_ids = new_item_ids[i:i + BATCH_SIZE]
                        bulk_define_abbrs.delay(doc_id, batch_ids)
                else:
                    from src.backend.tasks.public import bulk_define_terms
                    for i in range(0, len(new_item_ids), BATCH_SIZE):
                        batch_ids = new_item_ids[i:i + BATCH_SIZE]
                        bulk_define_terms.delay(doc_id, batch_ids)

                logger.info(
                    f"[EXTRACT] doc_id={doc_id}: Обработано {len(chunks)} чанков. "
                    f"Найдено {len(all_new_items)} {item_type}."
                )
            except Exception as e:
                db.rollback()
                logger.error(f"[EXTRACT] Ошибка БД для doc_id={doc_id}: {e}")