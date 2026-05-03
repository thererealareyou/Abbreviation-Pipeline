from celery import Celery
from celery.signals import setup_logging
from celery.utils.log import get_task_logger

from config import config
from src.backend.tasks.shared import _bulk_define, _bulk_extract, _bulk_resolve
from src.utils.logger import PipelineLogger, trace_id_var


@setup_logging.connect
def config_loggers(*args, **kwtags):
    PipelineLogger.setup_logging()

logger = get_task_logger(__name__)

app = Celery("nlp_pipeline", broker=config.CELERY_BROKER_URL)


@app.task(
    name="tasks.bulk_extract_terms", bind=True, max_retries=3, default_retry_delay=60
)
def bulk_extract_terms(
    self, doc_id: int, chunk_ids: list[int], trace_id: str = "unknown"
) -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [EXTRACT] [START] Термины: doc_id={doc_id}, chunks={len(chunk_ids)}")
        _bulk_extract(doc_id, chunk_ids, "term")
        logger.info(f"[TASK] [EXTRACT] [FINISH] Термины: doc_id={doc_id}")
    except Exception as exc:
        logger.warning(f"[TASK] [EXTRACT] [RETRY] Термины: doc_id={doc_id}, причина: {exc}")
        raise self.retry(exc=exc)
    finally:
        trace_id_var.reset(token)


@app.task(
    name="tasks.bulk_extract_abbrs", bind=True, max_retries=3, default_retry_delay=60
)
def bulk_extract_abbrs(
    self, doc_id: int, chunk_ids: list[int], trace_id: str = "unknown"
) -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [EXTRACT] [START] Аббревиатуры: doc_id={doc_id}, chunks={len(chunk_ids)}")
        _bulk_extract(doc_id, chunk_ids, "abbr")
        logger.info(f"[TASK] [EXTRACT] [FINISH] Аббревиатуры: doc_id={doc_id}")
    except Exception as exc:
        logger.warning(f"[TASK] [EXTRACT] [RETRY] Аббревиатуры: doc_id={doc_id}, причина: {exc}")
        raise self.retry(exc=exc)
    finally:
        trace_id_var.reset(token)


@app.task(
    name="tasks.bulk_define_terms", bind=True, max_retries=3, default_retry_delay=60
)
def bulk_define_terms(
    self, doc_id: int, item_ids: list[int], trace_id: str = "unknown"
) -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [DEFINE] [START] Термины: doc_id={doc_id}, items={len(item_ids)}")
        _bulk_define(doc_id, item_ids, "term")
        logger.info(f"[TASK] [DEFINE] [FINISH] Термины: doc_id={doc_id}")
    except Exception as exc:
        logger.warning(f"[TASK] [DEFINE] [RETRY] Термины: doc_id={doc_id}, причина: {exc}")
        raise self.retry(exc=exc)
    finally:
        trace_id_var.reset(token)


@app.task(
    name="tasks.bulk_define_abbrs", bind=True, max_retries=3, default_retry_delay=60
)
def bulk_define_abbrs(
    self, doc_id: int, item_ids: list[int], trace_id: str = "unknown"
) -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info(f"[TASK] [DEFINE] [START] Аббревиатуры: doc_id={doc_id}, items={len(item_ids)}")
        _bulk_define(doc_id, item_ids, "abbr")
        logger.info(f"[TASK] [DEFINE] [FINISH] Аббревиатуры: doc_id={doc_id}")
    except Exception as exc:
        logger.warning(f"[TASK] [DEFINE] [RETRY] Аббревиатуры: doc_id={doc_id}, причина: {exc}")
        raise self.retry(exc=exc)
    finally:
        trace_id_var.reset(token)


@app.task(
    name="tasks.bulk_resolve_terms", bind=True, max_retries=3, default_retry_delay=60
)
def bulk_resolve_terms(self, trace_id: str = "unknown") -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info("[TASK] [RESOLVE] [START] Разрешение конфликтов терминов")
        _bulk_resolve("term")
        logger.info("[TASK] [RESOLVE] [FINISH] Термины")
    except Exception as exc:
        logger.warning(f"[TASK] [RESOLVE] [RETRY] Термины, причина: {exc}")
        raise self.retry(exc=exc)
    finally:
        trace_id_var.reset(token)


@app.task(
    name="tasks.bulk_resolve_abbrs", bind=True, max_retries=3, default_retry_delay=60
)
def bulk_resolve_abbrs(self, trace_id: str = "unknown") -> None:
    token = trace_id_var.set(trace_id)
    try:
        logger.info("[TASK] [RESOLVE] [START] Разрешение конфликтов аббревиатур")
        _bulk_resolve("abbr")
        logger.info("[TASK] [RESOLVE] [FINISH] Аббревиатуры")
    except Exception as exc:
        logger.warning(f"[TASK] [RESOLVE] [RETRY] Аббревиатуры, причина: {exc}")
        raise self.retry(exc=exc)
    finally:
        trace_id_var.reset(token)