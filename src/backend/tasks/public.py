import os

from src.backend.tasks.shared import _bulk_extract, _bulk_define, _bulk_resolve, _bulk_transliteration

from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
app = Celery("nlp_pipeline", broker=CELERY_BROKER_URL)


@app.task(
    name="tasks.bulk_extract_terms",
    bind=True,
    max_retries=3,
    default_retry_delay=60
)
def bulk_extract_terms(self, doc_id: int, chunk_ids: list[int]) -> None:
    try:
        _bulk_extract(doc_id, chunk_ids, "term")
    except Exception as exc:
        logger.warning(f"Ошибка в батче с терминами, пробую еще раз: {exc}")
        raise self.retry(exc=exc)


@app.task(
    name="tasks.bulk_extract_abbrs",
    bind=True,
    max_retries=3,
    default_retry_delay=60
)
def bulk_extract_abbrs(self, doc_id: int, chunk_ids: list[int]) -> None:
    try:
        _bulk_extract(doc_id, chunk_ids, "abbr")
    except Exception as exc:
        logger.warning(f"Ошибка в батче с аббревиатурами, пробую еще раз: {exc}")
        raise self.retry(exc=exc)


@app.task(
    name="tasks.bulk_define_terms",
    bind=True,
    max_retries=3,
    default_retry_delay=60
)
def bulk_define_terms(self, doc_id: int, item_ids: list[int]) -> None:
    try:
        logger.info(f"[DEFINE] [START] Запуск терминов, батч: {len(item_ids)} шт.")
        _bulk_define(doc_id, item_ids, "term")
    except Exception as exc:
        raise self.retry(exc=exc)


@app.task(
    name="tasks.bulk_define_abbrs",
    bind=True,
    max_retries=3,
    default_retry_delay=60
)
def bulk_define_abbrs(self, doc_id: int, item_ids: list[int]) -> None:
    try:
        logger.info(f"[DEFINE] [START] Запуск аббревиатур, батч: {len(item_ids)} шт.")
        _bulk_define(doc_id, item_ids, "abbr")
    except Exception as exc:
        raise self.retry(exc=exc)


@app.task(
    name="tasks.bulk_resolve_terms",
    bind=True,
    max_retries=3,
    default_retry_delay=60
)
def bulk_resolve_terms(self) -> None:
    try:
        _bulk_resolve("term")
    except Exception as exc:
        logger.warning(f"Ошибка при разрешении терминологичных конфликтов, пробую еще раз: {exc}")
        raise self.retry(exc=exc)


@app.task(
    name="tasks.bulk_resolve_abbrs",
    bind=True,
    max_retries=3,
    default_retry_delay=60
)
def bulk_resolve_abbrs(self) -> None:
    try:
        _bulk_resolve("abbr")
    except Exception as exc:
        logger.warning(f"Ошибка при разрешении аббревиатурных конфликтов, пробую еще раз: {exc}")
        raise self.retry(exc=exc)
