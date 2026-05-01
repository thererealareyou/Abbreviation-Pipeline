from fastapi import APIRouter, Depends, HTTPException

from sqlalchemy import select, insert, delete
from sqlalchemy.orm import Session

from src.backend.models import Document, Chunk, ExtractedItem
from src.backend.tasks.public import bulk_extract_abbrs, bulk_extract_terms
from src.utils.db import get_db
from src.backend.schemas import TextsRequest

from config import config


router = APIRouter(
    prefix="/documents"
)


@router.post("/extract")
def start_extraction(payload: TextsRequest, db: Session = Depends(get_db)):
    """
    Принимает массив текстовых чанков и запускает пайплайн извлечения. Выполнение этой функции может занять некоторое время.
    """
    if not payload.texts:
        raise HTTPException(status_code=400, detail="Массив текстов не может быть пустым.")

    try:
        CHUNK_BATCH = config.BATCH_SIZE

        stmt = select(Document).where(Document.filename == payload.document_id)
        doc = db.execute(stmt).scalar_one_or_none()

        if doc:
            if doc.status == "processing":
                raise HTTPException(status_code=409, detail="Документ уже находится в обработке.")

            db.execute(delete(Chunk).where(Chunk.doc_id == doc.id))

            doc.status = "processing"
            doc.term_search_done = False
            doc.abbr_search_done = False
            doc.term_defs_done = False
            doc.abbr_defs_done = False
            doc.term_conflicts_done = False
            doc.abbr_conflicts_done = False

            if hasattr(doc, 'final_dictionary'):
                doc.final_dictionary = None

            for col in [
                "finding_abbr_chunks", "finding_term_chunks", "defining_abbrs",
                "defining_terms", "total_abbr_conflicts", "total_term_conflicts",
                "term_batches_total", "term_batches_done", "abbr_batches_total", "abbr_batches_done"
            ]:
                setattr(doc, col, 0)
        else:
            doc = Document(filename=payload.document_id, status="processing")
            db.add(doc)

        db.flush()

        chunks_data = [
            {"doc_id": doc.id, "text": t, "order": i}
            for i, t in enumerate(payload.texts)
        ]

        chunk_ids = db.scalars(
            insert(Chunk).returning(Chunk.id),
            chunks_data
        ).all()

        total_batches = (len(payload.texts) + CHUNK_BATCH - 1) // CHUNK_BATCH
        doc.term_batches_total = total_batches
        doc.abbr_batches_total = total_batches

        db.commit()

        stmt = select(Chunk.id).where(Chunk.doc_id == doc.id).order_by(Chunk.order)
        all_chunk_ids = db.scalars(stmt).all()

        for i in range(0, len(all_chunk_ids), CHUNK_BATCH):
            batch = list(all_chunk_ids[i:i + CHUNK_BATCH])
            bulk_extract_terms.delay(doc.id, batch)
            bulk_extract_abbrs.delay(doc.id, batch)

        return {
            "document_id": payload.document_id,
            "status": "Task submitted to queue",
            "message": f"Пайплайн запущен. Чанков: {len(payload.texts)}, батчей: {total_batches}.",
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка базы данных: {str(e)}")

@router.get("/result/{document_id}")
def get_result(
        document_id: str,
        target: str,
        limit: int = 100,
        offset: int = 0,
        db: Session = Depends(get_db)
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
    if target not in ("abbr", "term"):
        raise HTTPException(
            status_code=400,
            detail="Параметр target должен быть 'abbr' или 'term'.",
        )
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="limit должен быть от 1 до 1000.")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset не может быть отрицательным.")

    try:
        doc = db.query(Document).filter_by(filename=document_id).first()
        if not doc:
            raise HTTPException(status_code=404, detail="Документ не найден.")
        if doc.status != "completed":
            raise HTTPException(
                status_code=425,
                detail=f"Документ ещё не готов (статус: {doc.status}).",
            )

        if target in ("abbr", "term"):
            stmt = (
                select(ExtractedItem.word, ExtractedItem.definition)
                .join(Chunk)
                .where(
                    Chunk.doc_id == doc.id,
                    ExtractedItem.item_type == target,
                    ExtractedItem.definition.isnot(None)
                )
                .order_by(ExtractedItem.word)
                .offset(offset).limit(limit)
            )
            result = db.execute(stmt).all()
            data = {row.word: row.definition for row in result}

        else:
            raise HTTPException(status_code=400, detail="Некорректный тип данных.")

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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении результата: {str(e)}")


@router.delete("/delete/{document_name}")
def delete_document(document_name: str, db: Session = Depends(get_db)):
    """
    Удаляет документ и все связанные с ним данные.

    Каскадно удаляет чанки, извлечённые сущности и транслитерационные записи.
    Нельзя удалить документ в статусе 'processing'.

    Args:
        document_name: имя документа.

    Returns:
        Подтверждение удаления.
    """
    try:
        stmt = select(Document).where(Document.filename == document_name)
        result = db.execute(stmt)
        doc = result.scalar_one_or_none()

        if not doc:
            raise HTTPException(status_code=404, detail="Документ с таким именем не найден.")

        if doc.status == "processing":
            raise HTTPException(
                status_code=409,
                detail="Документ еще обрабатывается. Удаление запрещено.",
            )

        db.delete(doc)
        db.commit()

        return {
            "status": "success",
            "message": f"Документ {document_name} успешно удален."
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Ошибка сервера: {str(e)}")