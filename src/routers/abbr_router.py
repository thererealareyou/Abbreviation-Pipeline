import os
import json
import zipfile
import pandas as pd
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse

from src.schemas.request_models import TextInput
from src.schemas.response_models import SuccessResponse

# Импорты из вашего проекта
import hands
from main import load_config, main as run_pipeline
from src.extraction.model_client import AsyncAPIModelClient
from src.extraction.extraction_pipeline import build_initial_dataframe_async

router = APIRouter(tags=["Pipeline Operations"])


@router.post("/process-text", response_model=SuccessResponse)
async def process_text(input_data: TextInput):
    """
    Эндпоинт 1: Принимает текст, прогоняет через 1-й этап и сохраняет результат в SQL-таблицу.
    """
    config = load_config()

    # Инициализация клиента модели
    model = AsyncAPIModelClient(
        url=config["llm"]["api"]["url"],
        max_parallel=config["llm"]["api"]["max_parallel"],
        temperature=config["llm"]["api"]["temperature"]
    )

    # Этап 1: обработка одного чанка текста
    chunks = [input_data.text]
    try:
        df = await build_initial_dataframe_async(chunks, model, config)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обработки LLM: {e}")

    # Сохраняем результат в базу данных с использованием подключения из hands.py
    if not df.empty:
        row = df.iloc[0]
        # Сериализуем списки/словари в JSON-строку для записи в текстовое поле БД
        abbrs_str = json.dumps(row.get('abbrs', []), ensure_ascii=False)
        terms_str = json.dumps(row.get('terms', []), ensure_ascii=False)

        conn = hands.get_connection()
        if isinstance(conn, Exception):
            raise HTTPException(status_code=500, detail="Ошибка подключения к БД")

        try:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {config['db']['table_name']} 
                (sentence_content, abbrs, terms) 
                VALUES (?, ?, ?)
                """,
                (input_data.text, abbrs_str, terms_str)
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Ошибка БД: {e}")
        finally:
            conn.close()

    return SuccessResponse(status="success", message="Текст успешно обработан и добавлен в базу данных")


@router.post("/run-pipeline")
async def run_remaining_stages():
    """
    Эндпоинт 2: Без входа. Выгружает данные из БД, запускает этапы 2-4 и возвращает zip с JSON результатами.
    """
    config = load_config()

    # 1. Выгрузка данных из SQLite в excel-файл, который ожидает stage_2
    conn = hands.get_connection()
    if isinstance(conn, Exception):
        raise HTTPException(status_code=500, detail="Ошибка подключения к БД")

    try:
        query = f"SELECT sentence_content as chunk, abbrs, terms FROM {config['db']['table_name']}"
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения из БД: {e}")
    finally:
        conn.close()

    # Десериализуем JSON-строки обратно в списки для корректной работы пайплайна
    df['abbrs'] = df['abbrs'].apply(lambda x: json.loads(x) if isinstance(x, str) else [])
    df['terms'] = df['terms'].apply(lambda x: json.loads(x) if isinstance(x, str) else [])

    interim_path = str(os.path.join(config["paths"]["interim_data"]["folder"],
                                    config["paths"]["interim_data"]["sentences_xlsx"]))
    os.makedirs(os.path.dirname(interim_path), exist_ok=True)
    df.to_excel(interim_path, index=False)

    # 2. Запуск остальных этапов (2, 3 и 4)
    try:
        await run_pipeline(stage_1=False, stage_2=True, stage_3=True, stage_4=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка выполнения пайплайна: {e}")

    # 3. Упаковка результатов 4-го этапа в ZIP
    processed_folder = config["paths"]["processed"]["folder"]
    final_gloss_path = os.path.join(processed_folder, config["paths"]["processed"]["final_gloss_json"])
    eng_to_ru_path = os.path.join(processed_folder, config["paths"]["processed"]["eng_to_ru_json"])

    zip_filename = "pipeline_results.zip"
    zip_path = os.path.join(processed_folder, zip_filename)

    with zipfile.ZipFile(zip_path, 'w') as zipf:
        if os.path.exists(final_gloss_path):
            zipf.write(final_gloss_path, arcname=os.path.basename(final_gloss_path))
        if os.path.exists(eng_to_ru_path):
            zipf.write(eng_to_ru_path, arcname=os.path.basename(eng_to_ru_path))

    if not os.path.exists(zip_path):
        raise HTTPException(status_code=404, detail="Файлы с результатами не найдены")

    return FileResponse(zip_path, media_type="application/zip", filename=zip_filename)
