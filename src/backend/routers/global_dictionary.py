from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, select, update, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.backend.models import (GlobalDictionary, SystemState,
                                TransliterationDictionary)
from src.backend.tasks.public import bulk_resolve_abbrs, bulk_resolve_terms
from src.utils.db import get_db
from src.utils.logger import PipelineLogger

router = APIRouter(prefix="/global_dictionary")
logger = PipelineLogger.get_logger(__name__)


@router.post("/build")
def build_dictionary(db: Session = Depends(get_db)):
    """
    Запускает сборку глобального словаря ПО ВСЕМ ДОКУМЕНТАМ, разрешая конфликты. Выполнение этой функции может занять некоторое время.

    :return: Словарь с id celery-задач.
    """
    logger.info("[DICT] [BUILD] [REQUEST] Запрос на сборку глобального словаря")
    try:
        build_state = (
            db.execute(
                select(SystemState)
                .where(SystemState.key.in_(["build_term", "build_abbr"]))
                .with_for_update()
            )
            .scalars()
            .all()
        )

        if any(s.value == "processing" for s in build_state):
            logger.warning(
                "[DICT] [BUILD] [CONFLICT] Сборка уже выполняется, повторный запрос отклонён"
            )
            raise HTTPException(
                status_code=425,
                detail="Словарь уже строится, но ещё не готов.",
            )

        db.execute(
            update(SystemState)
            .where(SystemState.key.in_(["build_term", "build_abbr"]))
            .values(value="processing")
        )

        db.commit()
        logger.info("[DICT] [BUILD] [INFO] Состояние сборки установлено в 'processing'")

        try:
            task_abbrs = bulk_resolve_abbrs.delay()
            task_terms = bulk_resolve_terms.delay()
            logger.info(
                f"[DICT] [BUILD] [START] Запущены задачи: abbrs={task_abbrs.id}, terms={task_terms.id}"
            )
        except Exception as task_exc:
            logger.error(
                f"[DICT] [BUILD] [ERROR] Ошибка запуска Celery задач: {task_exc}",
                exc_info=True,
            )
            try:
                db.execute(
                    update(SystemState)
                    .where(SystemState.key.in_(["build_term", "build_abbr"]))
                    .values(value="idle")
                )
                db.commit()
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
            "task_ids": {"terms": task_terms.id, "abbrs": task_abbrs.id},
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
def get_result(
    target: str, limit: int = 100, offset: int = 0, db: Session = Depends(get_db)
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
        build_state = (
            db.execute(
                select(SystemState).where(
                    SystemState.key.in_(["build_term", "build_abbr"])
                )
            )
            .scalars()
            .all()
        )

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
            result = db.execute(stmt).all()
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
            result = db.execute(stmt).all()
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
def delete_dictionary(db: Session = Depends(get_db)):
    """
    Полностью очищает глобальный словарь и связанные данные.
    """
    logger.info("[DICT] [DELETE] [REQUEST] Запрос на удаление словаря")
    try:
        build_state = (
            db.execute(
                select(SystemState).where(
                    SystemState.key.in_(["build_term", "build_abbr"])
                )
            )
            .scalars()
            .all()
        )

        if any(s.value == "processing" for s in build_state):
            logger.warning("[DICT] [DELETE] [CONFLICT] Удаление запрещено: идёт сборка словаря")
            raise HTTPException(
                status_code=409,
                detail="Словарь сейчас обновляется. Удаление запрещено.",
            )

        deleted_global = db.query(func.count(GlobalDictionary.id)).scalar() or 0
        deleted_translit = db.query(func.count(TransliterationDictionary.id)).scalar() or 0

        db.execute(delete(GlobalDictionary))
        db.execute(delete(TransliterationDictionary))
        db.commit()

        logger.info(
            f"[DICT] [DELETE] [RESULT] Удалено записей: global={deleted_global}, translit={deleted_translit}"
        )

        return {"status": "success", "message": "Глобальный словарь полностью очищен."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка сервера: {str(e)}")
