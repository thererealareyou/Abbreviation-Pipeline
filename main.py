import asyncio
import os
import yaml
import pandas as pd
import json

from src.extraction.model_client import AsyncAPIModelClient
from src.extraction.pdf_parser import extract_sentences_from_folder
from src.dataset_builder.dataset_examiner import aggregate_definitions
from src.extraction.extraction_pipeline import (build_initial_dataframe_async,
                                                enrich_with_definitions_async,
                                                resolve_conflicts_async)
from src.utils.io_helpers import export_df_to_json


def load_config():
    with open("config/settings.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


async def main():
    config = load_config()

    # 1. Инициализация
    chunks = extract_sentences_from_folder(config["paths"]["raw_data"])[:100]
    model = AsyncAPIModelClient(
        url=config["llm"]["api"]["url"],
        temperature=config["llm"]["settings"]["temperature"],
        max_parallel=config["llm"]["api"]["max_parallel"]
    )
    # ==========================================
    # ЭТАП 1: ИЗВЛЕЧЕНИЕ
    # ==========================================
    df = await build_initial_dataframe_async(chunks, model, config)

    # 1.1. Сохранение и загрузка промежуточных данных
    interim_path = os.path.join(config["paths"]["interim_data"], f"{config['files']['interim_file']}.xlsx")
    df.to_excel(interim_path, columns=["chunk", "abbrs", "terms"], index=False)

    # ==========================================
    # ЭТАП 2: ПОЛУЧЕНИЕ ОПРЕДЕЛЕНИЙ
    # ==========================================

    # 2.1. Получение валидационных данных
    df = pd.read_excel(interim_path)
    df_enriched = await enrich_with_definitions_async(df, model, config)

    # 2.2. Сохранение валидационных данных
    validation_path = str(os.path.join(config["paths"]["validation"], config["files"]["extended_validation"]))
    df_enriched.to_json(validation_path, force_ascii=False, orient='records', indent=4)
    
    validation_path = str(os.path.join(config["paths"]["validation"], config["files"]["validation_glossary"]))
    mapping = {
        "abbreviations": "abbr_definitions",
        "terms": "term_definitions",
        "dropped_abbreviations": "dropped_abbr",
        "dropped_terms": "dropped_terms"
    }
    export_df_to_json(df_enriched, mapping, validation_path)
    # ==========================================
    # ЭТАП 3: ФОРМИРОВАНИЕ ИТОГОВОГО ГЛОССАРИЯ
    # ==========================================

    # 3.1. Получаем глоссарий с конфликтами из файла
    validation_path = str(os.path.join(config["paths"]["validation"], config["files"]["validation_glossary"]))
    df_val = pd.read_json(validation_path, typ="series")

    # 3.2. Собираем все термины из колонки 'terms' и их определения из 'term_definitions'
    final_abbrs, conflict_abbrs = aggregate_definitions(df_val["abbreviations"])
    final_terms, conflict_terms = aggregate_definitions(df_val["terms"])

    # 3.3. Разрешаем конфликты параллельно
    resolved_abbrs, resolved_terms = await resolve_conflicts_async(
        conflict_abbrs,
        conflict_terms,
        model,
        config
    )

    # 3.4. Обновляем финальные словари разрешенными данными
    final_abbrs.update(resolved_abbrs)
    final_terms.update(resolved_terms)

    # 3.5. Сохраняем итоговый словарь
    final_data = {
        "abbreviations": {k: [v] if isinstance(v, str) else v for k, v in final_abbrs.items()},
        "terms": {k: [v] if isinstance(v, str) else v for k, v in final_terms.items()}
    }

    processed_path = str(os.path.join(config["paths"]["processed"], config["files"]["processed_file"]))

    with open(processed_path, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    asyncio.run(main())
