from arq import create_pool
from arq.connections import RedisSettings
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import delete, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from config import config, DOCUMENT_PIPELINE_CONFIG
import uuid
from src.backend.models import Chunk, Document, ExtractedItem
from src.backend.schemas import TextsRequest
from src.backend.tasks.manager import pipeline_manager
from src.utils.db import get_db
from src.utils.logger import PipelineLogger

router = APIRouter(prefix="/documents")
logger = PipelineLogger.get_logger(__name__)

CHUNK_BATCH = config.BATCH_SIZE


@router.post("/extract")
async def start_extraction(payload: TextsRequest, request: Request, db: AsyncSession = Depends(get_db)):
    """
    Принимает массив текстовых чанков и запускает пайплайн извлечения. Выполнение этой функции может занять некоторое время.
    """
    trace_id = str(uuid.uuid4())

    logger.info(
        f"[DOC] [EXTRACT] [REQUEST] Запрос на извлечение: doc_id={payload.document_id}, texts={len(payload.texts)}, trace_id={trace_id}"
    )

    if not payload.texts:
        logger.warning("[DOC] [EXTRACT] [VALIDATION] Пустой массив текстов")
        raise HTTPException(
            status_code=400, detail="Массив текстов не может быть пустым."
        )

    try:
        stmt = select(Document).where(Document.filename == payload.document_id)
        rslt = await db.execute(stmt)
        doc = rslt.scalar_one_or_none()

        if doc:
            logger.info(f"[DOC] [EXTRACT] [LOOKUP] Найден документ id={doc.id}, статус={doc.status}")
            if doc.status == "processing":
                logger.warning(
                    f"[DOC] [EXTRACT] [CONFLICT] Документ {payload.document_id} уже обрабатывается"
                )
                raise HTTPException(
                    status_code=409, detail="Документ уже находится в обработке."
                )

            logger.info(f"[DOC] [EXTRACT] [CLEAN] Удаление старых чанков для doc_id={doc.id}")
            await db.execute(delete(Chunk).where(Chunk.doc_id == doc.id))
            doc.status = "processing"
            doc.term_search_done = False
            doc.abbr_search_done = False
            doc.term_defs_done = False
            doc.abbr_defs_done = False
            doc.term_conflicts_done = False
            doc.abbr_conflicts_done = False

            if hasattr(doc, "final_dictionary"):
                doc.final_dictionary = None

            for col in [
                "finding_abbr_chunks",
                "finding_term_chunks",
                "defining_abbrs",
                "defining_terms",
                "total_abbr_conflicts",
                "total_term_conflicts",
                "term_batches_total",
                "term_batches_done",
                "abbr_batches_total",
                "abbr_batches_done",
            ]:
                setattr(doc, col, 0)
        else:
            logger.info(f"[DOC] [EXTRACT] [NEW] Создание нового документа {payload.document_id}")
            doc = Document(filename=payload.document_id, status="processing")
            db.add(doc)
        await db.flush()

        new_chunks = [
            Chunk(doc_id=doc.id, text=text_content, order=idx)  # type: ignore
            for idx, text_content in enumerate(payload.texts)
        ]
        db.add_all(new_chunks)
        await db.flush()

        logger.info(f"[DOC] [EXTRACT] [CHUNKS] Создано чанков: {len(new_chunks)}")

        total_batches = (len(payload.texts) + CHUNK_BATCH - 1) // CHUNK_BATCH
        doc.term_batches_total = total_batches
        doc.abbr_batches_total = total_batches
        await db.commit()

        logger.info(
            f"[DOC] [EXTRACT] [BATCH] Всего батчей: {total_batches} (размер батча: {CHUNK_BATCH})"
        )

        arq_pool = request.app.state.arq_pool

        try:
            await pipeline_manager.start(
                redis=arq_pool,
                db=db,
                doc_id=doc.id,
                config=DOCUMENT_PIPELINE_CONFIG,
                trace_id=trace_id
            )
            logger.info(f"[DOC] [EXTRACT] [SUCCESS] Пайплайн успешно инициализирован для doc_id={doc.id}")

        except Exception as task_err:
            logger.error(
                f"[DOC] [EXTRACT] [ERROR] Ошибка при отправке ARQ-задач для doc_id={doc.id}: {task_err}",
                exc_info=True,
            )
            try:
                doc.status = "error"
                await db.commit()
                logger.warning("[DOC] [EXTRACT] [RECOVER] Статус документа изменён на 'error'")
            except Exception:
                await db.rollback()
                logger.error(
                    "[DOC] [EXTRACT] [ERROR] Не удалось обновить статус документа на 'error'"
                )
                raise HTTPException(
                    status_code=500,
                    detail="Ошибка при постановке задач в очередь.",
                )

        return {
            "document_id": payload.document_id,
            "status": "Task submitted to queue",
            "message": f"Пайплайн запущен. Чанков: {len(payload.texts)}, батчей: {total_batches}.",
        }

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(
            f"[DOC] [EXTRACT] [ERROR] Ошибка БД при запуске пайплайна для {payload.document_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Ошибка базы данных при запуске пайплайна")
    except Exception as e:
        await db.rollback()
        logger.error(
            f"[DOC] [EXTRACT] [ERROR] Неожиданная ошибка: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/result/{document_id}")
async def get_result(
        document_id: str,
        target: str,
        limit: int = 100,
        offset: int = 0,
        db: AsyncSession = Depends(get_db),
):
    """
    Возвращает итоговый словарь или транслитерационную таблицу для документа.

    Документ должен находиться в статусе 'completed'.
    Поддерживает пагинацию через параметры limit и offset.

    Args:
        document_id: идентификатор документа.
        target: тип возвращаемых данных — 'abbr', 'term' или 'transliteration'.
        limit: максимальное количество записей (по умолчанию 100).
        offset: смещение для пагинации (по умолчанию 0).

    Returns:
        Словарь {слово: определение}.
    """
    logger.info(
        f"[DOC] [RESULT] [REQUEST] Запрос результата: doc={document_id}, target={target}, limit={limit}, offset={offset}"
    )

    if target not in ("abbr", "term"):
        logger.warning(f"[DOC] [RESULT] [VALIDATION] Некорректный target: {target}")
        raise HTTPException(
            status_code=400,
            detail="Параметр target должен быть 'abbr' или 'term'.",
        )
    if limit < 1 or limit > 1000:
        logger.warning(f"[DOC] [RESULT] [VALIDATION] Некорректный limit: {limit}")
        raise HTTPException(status_code=400, detail="limit должен быть от 1 до 1000.")
    if offset < 0:
        logger.warning(f"[DOC] [RESULT] [VALIDATION] Некорректный offset: {offset}")
        raise HTTPException(
            status_code=400, detail="offset не может быть отрицательным."
        )

    try:
        stmt = select(Document).where(Document.filename == document_id)
        rslt = await db.execute(stmt)
        doc = rslt.scalar_one_or_none()

        if not doc:
            logger.warning(f"[DOC] [RESULT] [LOOKUP] Документ {document_id} не найден")
            raise HTTPException(status_code=404, detail="Документ не найден.")
        if doc.status != "completed":
            logger.info(
                f"[DOC] [RESULT] [CONFLICT] Документ {document_id} ещё не готов (статус: {doc.status})"
            )
            raise HTTPException(
                status_code=425,
                detail=f"Документ ещё не готов (статус: {doc.status}).",
            )

        stmt = (
            select(ExtractedItem.word, ExtractedItem.definition)
            .join(Chunk)
            .where(
                Chunk.doc_id == doc.id,
                ExtractedItem.item_type == target,
                ExtractedItem.definition.isnot(None),
            )
            .order_by(ExtractedItem.word)
            .offset(offset)
            .limit(limit)
        )
        rslt = await db.execute(stmt)
        rows = rslt.all()
        data = {row.word: row.definition for row in rows}

        logger.info(
            f"[DOC] [RESULT] [RESULT] Получено записей: {len(data)} для target={target}"
        )

        return {
            "document_id": document_id,
            "target": target,
            "offset": offset,
            "limit": limit,
            "count": len(data),
            "data": data,
        }

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            f"[DOC] [RESULT] [ERROR] Ошибка БД при получении результата для {document_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Ошибка базы данных при получении результата")
    except Exception as e:
        logger.error(
            f"[DOC] [RESULT] [ERROR] Неожиданная ошибка: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.delete("/delete/{document_name}")
async def delete_document(document_name: str, db: AsyncSession = Depends(get_db)):
    """
    Удаляет документ и все связанные с ним данные.

    Каскадно удаляет чанки, извлечённые сущности и транслитерационные записи.
    Нельзя удалить документ в статусе 'processing'.

    Args:
        document_name: имя документа.

    Returns:
        Подтверждение удаления.
    """
    logger.info(f"[DOC] [DELETE] [REQUEST] Запрос на удаление документа: {document_name}")

    try:
        stmt = select(Document).where(Document.filename == document_name)
        rslt = await db.execute(stmt)
        doc = rslt.scalar_one_or_none()

        if not doc:
            logger.warning(f"[DOC] [DELETE] [LOOKUP] Документ {document_name} не найден")
            raise HTTPException(
                status_code=404, detail="Документ с таким именем не найден."
            )

        if doc.status == "processing":
            logger.warning(
                f"[DOC] [DELETE] [CONFLICT] Попытка удалить обрабатываемый документ {document_name}"
            )
            raise HTTPException(
                status_code=409,
                detail="Документ еще обрабатывается. Удаление запрещено.",
            )

        doc_id = doc.id
        await db.delete(doc)
        await db.commit()

        logger.info(f"[DOC] [DELETE] [RESULT] Документ {document_name} (id={doc_id}) успешно удалён")
        return {
            "status": "success",
            "message": f"Документ {document_name} успешно удален.",
        }

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(
            f"[DOC] [DELETE] [ERROR] Ошибка БД при удалении документа {document_name}: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Ошибка базы данных при удалении документа")
    except Exception as e:
        await db.rollback()
        logger.error(
            f"[DOC] [DELETE] [ERROR] Неожиданная ошибка: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")
