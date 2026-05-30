import logging
from arq import Retry

from arq.connections import RedisSettings

from config import config
from src.backend.tasks.manager import pipeline_manager
from src.backend.tasks.shared import _bulk_define, _bulk_extract, _bulk_resolve, _bulk_transliterate
from src.utils.db import AsyncSessionLocal
from src.utils.logger import PipelineLogger, trace_id_var

logger = logging.getLogger('arq.user')


async def bulk_extract_terms(
        ctx, doc_id: int, chunk_ids: list[int], trace_id: str = "Unknown"
) -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [EXTRACT] [START] Термины: doc_id={doc_id}, chunks={len(chunk_ids)}")
        await _bulk_extract(doc_id, chunk_ids, "term")
        logger.info(f"[TASK] [EXTRACT] [FINISH] Термины: doc_id={doc_id}")
        async with AsyncSessionLocal() as db:
            await pipeline_manager.complete_batch(ctx, db, doc_id, trace_id)
    except Exception as exc:
        logger.warning(...)
        if ctx['job_try'] < 3:
            raise Retry(defer=60)
        raise exc
    finally:
        trace_id_var.reset(token)


async def bulk_extract_abbrs(
        ctx, doc_id: int, chunk_ids: list[int], trace_id: str = "Unknown"
) -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [EXTRACT] [START] Аббревиатуры: doc_id={doc_id}, chunks={len(chunk_ids)}")
        await _bulk_extract(doc_id, chunk_ids, "abbr")
        logger.info(f"[TASK] [EXTRACT] [FINISH] Аббревиатуры: doc_id={doc_id}")
        async with AsyncSessionLocal() as db:
            await pipeline_manager.complete_batch(ctx, db, doc_id, trace_id)
    except Exception as exc:
        logger.warning(f"[TASK] [EXTRACT] [RETRY] Аббревиатуры: doc_id={doc_id}, причина: {exc}")
        if ctx['job_try'] < 3:
            raise Retry(defer=60)
        raise exc
    finally:
        trace_id_var.reset(token)


async def bulk_define_terms(
        ctx, doc_id: int, item_ids: list[int], trace_id: str = "Unknown"
) -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [DEFINE] [START] Термины: doc_id={doc_id}, items={len(item_ids)}")
        await _bulk_define(doc_id, item_ids, "term")
        logger.info(f"[TASK] [DEFINE] [FINISH] Термины: doc_id={doc_id}")
        async with AsyncSessionLocal() as db:
            await pipeline_manager.complete_batch(ctx, db, doc_id, trace_id)
    except Exception as exc:
        logger.warning(f"[TASK] [DEFINE] [RETRY] Термины: doc_id={doc_id}, причина: {exc}")
        if ctx['job_try'] < 3:
            raise Retry(defer=60)
        raise exc
    finally:
        trace_id_var.reset(token)


async def bulk_define_abbrs(
        ctx, doc_id: int, item_ids: list[int], trace_id: str = "Unknown"
) -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [DEFINE] [START] Аббревиатуры: doc_id={doc_id}, items={len(item_ids)}")
        await _bulk_define(doc_id, item_ids, "abbr")
        logger.info(f"[TASK] [DEFINE] [FINISH] Аббревиатуры: doc_id={doc_id}")
        async with AsyncSessionLocal() as db:
            await pipeline_manager.complete_batch(ctx, db, doc_id, trace_id)
    except Exception as exc:
        logger.warning(f"[TASK] [DEFINE] [RETRY] Аббревиатуры: doc_id={doc_id}, причина: {exc}")
        if ctx['job_try'] < 3:
            raise Retry(defer=60)
        raise exc
    finally:
        trace_id_var.reset(token)


async def bulk_resolve_terms(ctx, doc_id: int, trace_id: str = "Unknown") -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info("[TASK] [RESOLVE] [START] Разрешение конфликтов терминов")
        await _bulk_resolve(doc_id, "term")
        logger.info("[TASK] [RESOLVE] [FINISH] Термины")
        async with AsyncSessionLocal() as db:
            await pipeline_manager.complete_batch(ctx, db, doc_id, trace_id)
    except Exception as exc:
        logger.warning(f"[TASK] [RESOLVE] [RETRY] Термины, причина: {exc}")
        if ctx['job_try'] < 3:
            raise Retry(defer=60)
        raise exc
    finally:
        trace_id_var.reset(token)


async def bulk_resolve_abbrs(ctx, doc_id: int, trace_id: str = "unknown") -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info("[TASK] [RESOLVE] [START] Разрешение конфликтов аббревиатур")
        await _bulk_resolve(doc_id, "abbr")
        logger.info("[TASK] [RESOLVE] [FINISH] Аббревиатуры")
        async with AsyncSessionLocal() as db:
            await pipeline_manager.complete_batch(ctx, db, doc_id, trace_id)
    except Exception as exc:
        logger.warning(f"[TASK] [RESOLVE] [RETRY] Аббревиатуры, причина: {exc}")
        if ctx['job_try'] < 3:
            raise Retry(defer=60)
        raise exc
    finally:
        trace_id_var.reset(token)


async def bulk_transliterate_abbrs(ctx, doc_id: int, trace_id: str = "Unknown") -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [TRANSLITERATE] [START] Транслитерация для doc_id={doc_id}")
        await _bulk_transliterate(doc_id)
        logger.info(f"[TASK] [TRANSLITERATE] [FINISH] Транслитерация для doc_id={doc_id}")
        async with AsyncSessionLocal() as db:
            await pipeline_manager.complete_batch(ctx, db, doc_id, trace_id)
    except Exception as exc:
        logger.error(f"[TASK] [TRANSLITERATE] [ERROR] Ошибка на этапе транслитерации doc_id={doc_id}: {exc}")
        raise exc
    finally:
        trace_id_var.reset(token)


async def startup(ctx):
    PipelineLogger.setup_logging()
    logger.info("[WORKER] Логгер успешно инициализирован, воркер запущен.")


async def shutdown(ctx):
    logger.info("[WORKER] Воркер останавливается...")


class WorkerSettings:
    redis_settings = RedisSettings.from_dsn(config.REDIS_URL)
    functions = [bulk_extract_terms, bulk_extract_abbrs,
                 bulk_define_terms, bulk_define_abbrs,
                 bulk_resolve_terms, bulk_resolve_abbrs,
                 bulk_transliterate_abbrs]
    on_startup = startup
    on_shutdown = shutdown

    max_jobs = config.MAX_WORKERS
