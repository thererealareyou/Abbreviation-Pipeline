import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from src.backend.models import (GlobalDictionary, SystemState,
                                TransliterationDictionary)
from src.backend.tasks.manager import pipeline_manager

from config import GLOBAL_DICT_PIPELINE_CONFIG
from src.utils.db import get_db
from src.utils.logger import PipelineLogger
from arq import ArqRedis

from src.utils.arq_utils import get_arq_pool

router = APIRouter(prefix="/global_dictionary")
logger = PipelineLogger.get_logger(__name__)


@router.post("/build")
async def build_dictionary(db: AsyncSession = Depends(get_db), arq_pool: ArqRedis = Depends(get_arq_pool)):
    """
    Запускает сборку глобального словаря ПО ВСЕМ ДОКУМЕНТАМ, разрешая конфликты. Выполнение этой функции может занять некоторое время.

    :return: Словарь с id ARQ-задач.
    """
    logger.info("[DICT] [BUILD] [REQUEST] Запрос на сборку глобального словаря")
    trace_id = str(uuid.uuid4())

    try:
        stmt = (
            select(SystemState)
            .where(SystemState.key.in_(["build_term", "build_abbr"]))
            .with_for_update()
        )
        rslt = await db.execute(stmt)
        build_state = rslt.scalars().all()

        if any(s.value == "processing" for s in build_state):
            logger.warning(
                "[DICT] [BUILD] [CONFLICT] Сборка уже выполняется, повторный запрос отклонён"
            )
            raise HTTPException(
                status_code=425,
                detail="Словарь уже строится, но ещё не готов.",
            )

        await db.execute(
            update(SystemState)
            .where(SystemState.key.in_(["build_term", "build_abbr"]))
            .values(value="processing")
        )

        await db.commit()

        logger.info("[DICT] [BUILD] [INFO] Состояние сборки установлено в 'processing'")

        try:
            await pipeline_manager.start(
                redis=arq_pool,
                db=db,
                doc_id=0,
                config=GLOBAL_DICT_PIPELINE_CONFIG,
                trace_id=trace_id
            )
            logger.info(
                f"[DICT] [BUILD] [START] Задача построения глобального словаря успешно запущена"
            )

        except Exception as task_exc:
            logger.error(
                f"[DICT] [BUILD] [ERROR] Ошибка запуска ARQ задач: {task_exc}",
                exc_info=True,
            )
            try:
                await db.execute(
                    update(SystemState)
                    .where(SystemState.key.in_(["build_term", "build_abbr"]))
                    .values(value="idle")
                )
                await db.commit()
                logger.warning("[DICT] [BUILD] [ROLLBACK] Состояние сборки возвращено в 'idle'")
            except Exception as rollback_exc:
                logger.error(
                    f"[DICT] [BUILD] [ERROR] Не удалось откатить состояние сборки: {rollback_exc}",
                    exc_info=True,
                )
            raise HTTPException(
                status_code=500,
                detail="Не удалось запустить фоновые задачи сборки словаря.",
            )

        return {
            "status": "processing",
            "message": "Сборка глобального словаря запущена в фоновом режиме.",
        }

    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(
            f"[DICT] [BUILD] [ERROR] Ошибка БД при запуске сборки: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Ошибка базы данных при запуске сборки")
    except Exception as e:
        logger.error(
            f"[DICT] [BUILD] [ERROR] Неожиданная ошибка при запуске сборки: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/result")
async def get_result(
        target: str, limit: int = 100, offset: int = 0, db: AsyncSession = Depends(get_db)
):
    """
    Возвращает итоговый словарь или транслитерационную таблицу для глобального документа.

    Документ должен находиться в статусе 'completed'.
    Поддерживает пагинацию через параметры limit и offset.

    Args:
        target: тип возвращаемых данных — 'abbr', 'term' или 'transliteration'.
        limit: максимальное количество записей (по умолчанию 100).
        offset: смещение для пагинации (по умолчанию 0).
        db: Передаётся по умолчанию

    Returns:
        Словарь {слово: определение} или {ru_вариант: аббревиатура} для транслитерации.
    """
    logger.info(
        f"[DICT] [RESULT] [REQUEST] Запрос словаря: target={target}, limit={limit}, offset={offset}"
    )
    if target not in ("abbr", "term", "transliteration"):
        logger.warning(f"[DICT] [RESULT] [VALIDATION] Некорректный target: {target}")
        raise HTTPException(
            status_code=400,
            detail="Параметр target должен быть 'abbr', 'term' или 'transliteration'.",
        )
    if limit < 1 or limit > 1000:
        logger.warning(f"[DICT] [RESULT] [VALIDATION] Некорректный limit: {limit}")
        raise HTTPException(status_code=400, detail="limit должен быть от 1 до 1000.")
    if offset < 0:
        logger.warning(f"[DICT] [RESULT] [VALIDATION] Некорректный offset: {offset}")
        raise HTTPException(
            status_code=400, detail="offset не может быть отрицательным."
        )

    try:
        stmt = select(SystemState).where(
            SystemState.key.in_(["build_term", "build_abbr"])
        )
        rslt = await db.execute(stmt)
        build_state = rslt.scalars().all()

        if any(s.value == "processing" for s in build_state):
            logger.info("[DICT] [RESULT] [CONFLICT] Сборка ещё не завершена.")
            raise HTTPException(
                status_code=425,
                detail="Словарь ещё не готов.",
            )

        if target in ("abbr", "term"):
            stmt = (
                select(GlobalDictionary.word, GlobalDictionary.definition)
                .where(GlobalDictionary.item_type == target)
                .order_by(GlobalDictionary.word)
                .offset(offset)
                .limit(limit)
            )
            rslt = await db.execute(stmt)
            result = rslt.all()
            data = {row.word: row.definition for row in result}

        else:
            stmt = (
                select(
                    TransliterationDictionary.ru_variant, TransliterationDictionary.abbr
                )
                .order_by(TransliterationDictionary.abbr)
                .offset(offset)
                .limit(limit)
            )
            rslt = await db.execute(stmt)
            result = rslt.all()

            data = {row.ru_variant: row.abbr for row in result}

        logger.info(
            f"[DICT] [RESULT] [RESULT] Получено записей: {len(data)} для target={target}"
        )

        return {
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
            f"[DICT] [RESULT] [ERROR] Ошибка БД при получении словаря (target={target}): {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Ошибка базы данных при получении результата")
    except Exception as e:
        logger.error(
            f"[DICT] [RESULT] [ERROR] Неожиданная ошибка: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.delete("/delete")
async def delete_dictionary(db: AsyncSession = Depends(get_db)):
    """
    Полностью очищает глобальный словарь и связанные данные.
    """
    logger.info("[DICT] [DELETE] [REQUEST] Запрос на удаление словаря")
    try:
        stmt = select(SystemState).where(
            SystemState.key.in_(["build_term", "build_abbr"])
        )
        rslt = await db.execute(stmt)
        build_state = rslt.scalars().all()

        if any(s.value == "processing" for s in build_state):
            logger.warning("[DICT] [DELETE] [CONFLICT] Удаление запрещено: идёт сборка словаря")
            raise HTTPException(
                status_code=409,
                detail="Словарь сейчас обновляется. Удаление запрещено.",
            )

        stmt = select(func.count(GlobalDictionary.id))
        rslt = await db.execute(stmt)
        deleted_global = rslt.scalar_one_or_none()

        stmt = select(func.count(TransliterationDictionary.id))
        rslt = await db.execute(stmt)
        deleted_translit = rslt.scalar_one_or_none()

        await db.execute(delete(GlobalDictionary))
        await db.execute(delete(TransliterationDictionary))
        await db.commit()

        logger.info(
            f"[DICT] [DELETE] [RESULT] Удалено записей: global={deleted_global}, translit={deleted_translit}"
        )

        return {"status": "success", "message": "Глобальный словарь полностью очищен."}

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка сервера: {str(e)}")
