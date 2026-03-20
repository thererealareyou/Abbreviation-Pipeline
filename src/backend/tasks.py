import asyncio
import json
import os

import yaml
import aiohttp
import re

from collections import defaultdict
from typing import Literal

from celery import Celery
from celery.utils.log import get_task_logger

from src.extraction.model_client import get_llm_client, parse_llm_definition_response
from src.extraction.regex_detector import clean_abbr_list, clean_terms_list, verify_expansion
from src.extraction.transliteration import build_transliteration_map
from src.backend.db import SessionLocal
from src.backend.models import Document, Chunk, ExtractedItem, TransliterationEntry

from sqlalchemy import text, update

BATCH_SIZE = 10

logger = get_task_logger(__name__)

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")

app = Celery("nlp_pipeline", broker=CELERY_BROKER_URL)

with open("config/settings.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

ItemType = Literal["term", "abbr"]

def _run_async(coro):
    """Создаёт отдельный event loop для каждого потока Celery."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)

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

# ---------------------------------------------------------------------------
# Stage 1-2: Поиск сущностей в чанках
# ---------------------------------------------------------------------------

async def _search_all_chunks_streaming(chunks: list, item_type: ItemType, doc_id: int) -> None:
    stage = _SEARCH_STAGE[item_type]
    instructions = config["llm"][stage]["instructions"]
    model = get_llm_client()
    sem = asyncio.Semaphore(1)
    buffer: list[ExtractedItem] = []
    chunks_in_batch = 0
    lock = asyncio.Lock()

    async def flush(items: list, chunks_count: int) -> None:
        db = SessionLocal()
        try:
            if items:
                db.bulk_save_objects(items)
            db.execute(
                update(Document)
                .where(Document.id == doc_id)
                .values(
                    **{f"finding_{item_type}_chunks": getattr(Document, f"finding_{item_type}_chunks") + chunks_count})
            )
            db.commit()
        finally:
            db.close()

    async def process(session: aiohttp.ClientSession, chunk) -> None:
        nonlocal chunks_in_batch
        async with sem:
            try:
                raw = await model.generate_async(
                    session, f"{instructions}\n{chunk.text}", stage=stage
                )
            except Exception as e:
                logger.error(f"[streaming] chunk_id={chunk.id}: {e}")
                return

            if not raw:
                return

            cleaned = clean_terms_list(chunk.text, raw) if item_type == "term" else clean_abbr_list(chunk.text, raw)
            logger.info(
                f"[streaming] chunk_id={chunk.id} item_type={item_type} | После clean: {cleaned} (всего: {len(cleaned)})")

            async with lock:
                chunks_in_batch += 1

                if cleaned:
                    buffer.extend([
                        ExtractedItem(chunk_id=chunk.id, item_type=item_type, word=w)
                        for w in cleaned
                    ])

                if len(buffer) >= BATCH_SIZE or (not cleaned and chunks_in_batch % BATCH_SIZE == 0):
                    await flush(buffer.copy(), chunks_in_batch)
                    buffer.clear()
                    chunks_in_batch = 0

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=40)
    ) as session:
        await asyncio.gather(*[process(session, c) for c in chunks])
        async with lock:
            await flush(buffer.copy(), chunks_in_batch)
            buffer.clear()

# ---------------------------------------------------------------------------
# Stage 3-4: Поиск определений
# ---------------------------------------------------------------------------

async def _fetch_definitions(
    items_with_context: list[tuple[int, str, str]],
    item_type: ItemType,
    doc_id: int,
) -> dict[int, str]:
    """Параллельно ищет определения. Возвращает {item_id: определение}."""
    if not items_with_context:
        return {}

    stage = _DEFINE_STAGE[item_type]
    instructions = config["llm"][stage]["instructions"]
    prompt_key = _DEFINE_PROMPT_KEY[item_type]
    model = get_llm_client()
    results: dict[int, str] = {}

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            model.generate_async(
                session,
                instructions.format(**{prompt_key: word, "chunk_text": chunk_text}),
                stage=stage,
            )
            for _, word, chunk_text in items_with_context
        ]
        raw_results = await asyncio.gather(*tasks)

    for (item_id, word, chunk_text), raw_res in zip(items_with_context, raw_results):
        if not raw_res:
            continue
        valid_def = parse_llm_definition_response(raw_res)
        if not valid_def:
            continue

        if item_type == "abbr" and not verify_expansion(abbr=word, expansion=valid_def, chunk_text=chunk_text):
            continue

        if word in valid_def or (item_type == "abbr" and re.search(rf"^{re.escape(word)}\s*[-—–]", valid_def)):
            continue

        logger.info(f"[_bulk_define] doc_id={doc_id} item_type={item_type} | Определение для \"{word}\": {valid_def}")
        results[item_id] = valid_def[:1].upper() + valid_def[1:]

    if results:
        db = SessionLocal()
        try:
            db.execute(
                update(Document)
                .where(Document.id == doc_id)
                .values(
                    **{f"defining_{item_type}s": getattr(Document, f"defining_{item_type}s") + len(items_with_context)})
            )
            db.commit()
        finally:
            db.close()

    return results

# ---------------------------------------------------------------------------
# Stage 5-6: Разрешение конфликтов
# ---------------------------------------------------------------------------

async def _resolve_conflicts(
    conflicts: dict[str, list[str]],
    item_type: ItemType,
) -> dict[str, str]:
    if not conflicts:
        return {}

    stage = _RESOLVE_STAGE[item_type]
    instructions = config["llm"][stage]["instructions"]
    model = get_llm_client()
    keys = list(conflicts.keys())
    sem = asyncio.Semaphore(1)

    async def resolve_one(session, word):
        async with sem:
            return await model.generate_async(
                session,
                instructions.replace("{ENTITY}", word).replace(
                    "{VARIANTS}", json.dumps(conflicts[word], ensure_ascii=False)
                ),
                stage=stage,
            )

    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        raw_results = await asyncio.gather(*[resolve_one(session, w) for w in keys])

    resolved: dict[str, str] = {}
    for word, raw_res in zip(keys, raw_results):
        try:
            if not raw_res:
                raise ValueError("Empty response")
            clean = raw_res.strip().replace("```json", "").replace("```", "")
            data = json.loads(clean)
            final_val = data.get("resolved_definition", "").strip()
            if not final_val:
                raise ValueError("Empty resolved_definition")
            resolved[word] = final_val
        except Exception as e:
            logger.warning(f"[resolve_conflicts] {item_type} '{word}': {e}. Беру первый вариант.")
            resolved[word] = conflicts[word][0]

    return resolved

# ---------------------------------------------------------------------------
# Общие Celery-таски
# ---------------------------------------------------------------------------

def _bulk_extract_batch(doc_id: int, chunk_ids: list[int], item_type: ItemType) -> None:
    db = SessionLocal()
    try:
        chunks = db.query(Chunk).filter(Chunk.id.in_(chunk_ids)).all()
        if not chunks:
            return

        _run_async(_search_all_chunks_streaming(chunks, item_type, doc_id))

        db.execute(
            update(Document)
            .where(Document.id == doc_id)
            .values(**{f"{item_type}_batches_done": Document.__table__.c[f"{item_type}_batches_done"] + 1})
        )
        db.commit()

        doc = db.query(Document).filter(Document.id == doc_id).first()
        batches_done = getattr(doc, f"{item_type}_batches_done")
        batches_total = getattr(doc, f"{item_type}_batches_total")

        logger.info(f"[bulk_extract_batch] doc_id={doc_id} item_type={item_type} | батч {batches_done}/{batches_total}")

        if batches_done >= batches_total:
            search_done_flag = _SEARCH_DONE_FLAG[item_type]
            doc = db.query(Document).filter(Document.id == doc_id).with_for_update().first()
            setattr(doc, search_done_flag, True)
            db.commit()
            logger.info(f"[bulk_extract_batch] {search_done_flag}=True для doc_id={doc_id}")

            if item_type == "term":
                bulk_define_terms.delay(doc_id)
            else:
                bulk_define_abbrs.delay(doc_id)

    except Exception as e:
        logger.error(f"[bulk_extract_batch] Критическая ошибка doc_id={doc_id}: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()

def _bulk_define(doc_id: int, item_type: ItemType) -> None:
    search_done_flag = _SEARCH_DONE_FLAG[item_type]
    defs_done_flag = _DEFS_DONE_FLAG[item_type]
    db = SessionLocal()
    try:
        doc = db.query(Document).filter(Document.id == doc_id).first()
        if not doc or not getattr(doc, search_done_flag) or getattr(doc, defs_done_flag):
            return

        rows = (
            db.query(ExtractedItem, Chunk.text)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc_id,
                ExtractedItem.item_type == item_type,
                ExtractedItem.definition.is_(None),
            )
            .all()
        )

        context_list = [(item.id, item.word, text) for item, text in rows]
        definitions = _run_async(_fetch_definitions(context_list, item_type, doc_id))

        for item, _ in rows:
            if item.id in definitions:
                logger.info(f"[bulk_define] item.id={item.id}, item={item}")
                item.definition = definitions[item.id]

        setattr(doc, defs_done_flag, True)
        db.commit()

        if item_type == "term":
            resolve_term_conflicts.delay(doc_id)
        else:
            resolve_abbr_conflicts.delay(doc_id)

    except Exception as e:
        logger.error(f"[bulk_define] Критическая ошибка doc_id={doc_id}: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def _resolve_conflicts_task(doc_id: int, item_type: ItemType) -> None:
    defs_done_flag = _DEFS_DONE_FLAG[item_type]
    conflicts_done_flag = _CONFLICTS_DONE_FLAG[item_type]
    db = SessionLocal()
    try:
        db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        doc = db.query(Document).filter(Document.id == doc_id).first()

        if not doc:
            return
        if not getattr(doc, defs_done_flag):
            raise RuntimeError(f"{defs_done_flag} not ready yet")
        if getattr(doc, conflicts_done_flag):
            return

        items = (
            db.query(ExtractedItem)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc_id,
                ExtractedItem.item_type == item_type,
                ExtractedItem.definition.isnot(None),
            )
            .all()
        )

        grouped: dict[str, set] = defaultdict(set)
        for i in items:
            grouped[i.word].add(i.definition.strip())

        conflicts: dict[str, list[str]] = {}
        final: dict[str, str] = {}
        for word, defs in grouped.items():
            if len(defs) == 1:
                final[word] = next(iter(defs))
            else:
                conflicts[word] = list(defs)

        db.execute(
            update(Document)
            .where(Document.id == doc_id)
            .values(**{f"total_{item_type}_conflicts": len(conflicts)})
        )
        db.commit()

        if conflicts:
            resolved = _run_async(_resolve_conflicts(conflicts, item_type))
            final.update(resolved)

        doc = db.query(Document).filter(Document.id == doc_id).with_for_update().first()

        for word, definition in final.items():
            subq = (
                db.query(ExtractedItem.id)
                .join(Chunk)
                .filter(
                    Chunk.doc_id == doc_id,
                    ExtractedItem.item_type == item_type,
                    ExtractedItem.word == word,
                )
                .limit(1)
                .scalar_subquery()
            )
            db.query(ExtractedItem).filter(
                ExtractedItem.id == subq
            ).update(
                {"definition": definition, "is_final": True},
                synchronize_session=False,
            )

        setattr(doc, conflicts_done_flag, True)
        db.commit()

        doc_refreshed = db.query(Document).filter(Document.id == doc_id).first()
        if doc_refreshed.term_conflicts_done and doc_refreshed.abbr_conflicts_done:
            build_transliteration.delay(doc_id)
            logger.info(f"[resolve_conflicts_task] Запущена транслитерация для doc_id={doc_id}")

    except Exception as e:
        logger.error(f"[resolve_conflicts_task] Критическая ошибка doc_id={doc_id}: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def _build_transliteration(doc_id: int) -> None:
    db = SessionLocal()
    try:
        doc = db.query(Document).filter(Document.id == doc_id).with_for_update().first()
        if not doc:
            logger.error(f"[build_transliteration] Документ {doc_id} не найден")
            return

        if not (doc.term_conflicts_done and doc.abbr_conflicts_done):
            logger.warning(f"[build_transliteration] Словарь ещё не готов для doc_id={doc_id}")
            return

        rows = (
            db.query(ExtractedItem.word, ExtractedItem.definition)
            .join(Chunk)
            .filter(
                Chunk.doc_id == doc_id,
                ExtractedItem.item_type == "abbr",
                ExtractedItem.is_final == True,
            )
            .all()
        )
        abbreviations = {word: definition for word, definition in rows}

        if not abbreviations:
            logger.info(f"[build_transliteration] Нет аббревиатур для транслитерации, doc_id={doc_id}")
            doc.status = "completed"
            db.commit()
            return

        translit_map = build_transliteration_map(abbreviations, 5)
        logger.info(f"[build_transliteration] Построено {len(translit_map)} вариантов для doc_id={doc_id}")

        db.query(TransliterationEntry).filter_by(doc_id=doc_id).delete()
        db.bulk_save_objects([
            TransliterationEntry(doc_id=doc_id, ru_variant=ru, abbr=abbr)
            for ru, abbr in translit_map.items()
        ])

        doc.status = "completed"
        db.commit()

    except Exception as e:
        logger.error(f"[build_transliteration] Критическая ошибка doc_id={doc_id}: {e}")
        db.rollback()
        raise
    finally:
        db.close()

# ---------------------------------------------------------------------------
# Публичные Celery-таски (вызываются через .delay())
# ---------------------------------------------------------------------------

@app.task(name="tasks.bulk_extract_terms_batch")
def bulk_extract_terms_batch(doc_id: int, chunk_ids: list[int]) -> None:
    _bulk_extract_batch(doc_id, chunk_ids, "term")


@app.task(name="tasks.bulk_extract_abbrs_batch")
def bulk_extract_abbrs_batch(doc_id: int, chunk_ids: list[int]) -> None:
    _bulk_extract_batch(doc_id, chunk_ids, "abbr")


@app.task(name="tasks.bulk_define_terms")
def bulk_define_terms(doc_id: int) -> None:
    _bulk_define(doc_id, "term")


@app.task(name="tasks.bulk_define_abbrs")
def bulk_define_abbrs(doc_id: int) -> None:
    _bulk_define(doc_id, "abbr")


@app.task(name="tasks.resolve_term_conflicts")
def resolve_term_conflicts(doc_id: int) -> None:
    _resolve_conflicts_task(doc_id, "term")


@app.task(name="tasks.resolve_abbr_conflicts")
def resolve_abbr_conflicts(doc_id: int) -> None:
    _resolve_conflicts_task(doc_id, "abbr")


@app.task(name="tasks.build_transliteration")
def build_transliteration(doc_id: int) -> None:
    _build_transliteration(doc_id)