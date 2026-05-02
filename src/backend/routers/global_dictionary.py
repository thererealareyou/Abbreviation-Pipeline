from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from src.backend.models import (GlobalDictionary, SystemState,
                                TransliterationDictionary)
from src.backend.tasks.public import bulk_resolve_abbrs, bulk_resolve_terms
from src.utils.db import get_db

router = APIRouter(prefix="/global_dictionary")


@router.post("/build")
def build_dictionary(db: Session = Depends(get_db)):
    """
    Запускает сборку глобального словаря ПО ВСЕМ ДОКУМЕНТАМ, разрешая конфликты. Выполнение этой функции может занять некоторое время.

    :return: Словарь с id celery-задач.
    """
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

        task_abbrs = bulk_resolve_abbrs.delay()
        task_terms = bulk_resolve_terms.delay()
        return {
            "status": "processing",
            "message": "Сборка глобального словаря запущена в фоновом режиме.",
            "task_ids": {"terms": task_terms.id, "abbrs": task_abbrs.id},
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Не удалось запустить сборку: {str(e)}"
        )


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
    if target not in ("abbr", "term", "transliteration"):
        raise HTTPException(
            status_code=400,
            detail="Параметр target должен быть 'abbr', 'term' или 'transliteration'.",
        )
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="limit должен быть от 1 до 1000.")
    if offset < 0:
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

        return {
            "target": target,
            "offset": offset,
            "limit": limit,
            "count": len(data),
            "data": data,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Ошибка при получении результата: {str(e)}"
        )


@router.delete("/delete")
def delete_dictionary(db: Session = Depends(get_db)):
    """
    Полностью очищает глобальный словарь и связанные данные.
    """
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
            raise HTTPException(
                status_code=409,
                detail="Словарь сейчас обновляется. Удаление запрещено.",
            )

        db.execute(delete(GlobalDictionary))
        db.execute(delete(TransliterationDictionary))
        db.commit()

        return {"status": "success", "message": "Глобальный словарь полностью очищен."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка сервера: {str(e)}")
