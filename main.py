import asyncio
import os
import yaml
import pandas as pd
import json

from src.utils.logger import PipelineLogger  # ИМПОРТИРУЕМ НАШ ЛОГГЕР
from src.extraction.model_client import AsyncAPIModelClient
from src.extraction.pdf_parser import extract_sentences_from_folder
from src.dataset_builder.dataset_examiner import aggregate_definitions
from src.extraction.extraction_pipeline import (build_initial_dataframe_async,
                                                enrich_with_definitions_async,
                                                resolve_conflicts_async)
from src.utils.io_helpers import export_df_to_json

# Инициализируем логгер для главного модуля
logger = PipelineLogger.get_logger(__name__)


def load_config():
    with open("config/settings.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


async def main():
    logger.info("=== ЗАПУСК ПАЙПЛАЙНА ИЗВЛЕЧЕНИЯ СУЩНОСТЕЙ ===")

    try:
        config = load_config()
        logger.debug("Конфигурация успешно загружена.")

        # 1. Инициализация и загрузка чанков
        logger.info("Чтение PDF-файлов и извлечение текста...")
        chunks = extract_sentences_from_folder(config["paths"]["raw_data"])[:100]  # Ограничение 100 для теста

        if not chunks:
            logger.warning("Не найдено ни одного чанка для обработки. Завершение работы.")
            return

        logger.info("Успешно извлечено %d чанков для обработки.", len(chunks))

        model = AsyncAPIModelClient(
            url=config["llm"]["api"]["url"],
            temperature=config["llm"]["settings"]["temperature"],
            max_parallel=config["llm"]["api"]["max_parallel"]
        )
        logger.info("LLM клиент инициализирован (max_parallel=%d).", config["llm"]["api"]["max_parallel"])

        # ==========================================
        # ЭТАП 1: ИЗВЛЕЧЕНИЕ
        # ==========================================
        logger.info("--- [ЭТАП 1/3] ИЗВЛЕЧЕНИЕ АББРЕВИАТУР И ТЕРМИНОВ ---")
        df = await build_initial_dataframe_async(chunks, model, config)

        interim_path = os.path.join(config["paths"]["interim_data"], f"{config['files']['interim_file']}.xlsx")
        df.to_excel(interim_path, columns=["chunk", "abbrs", "terms"], index=False)
        logger.info("Этап 1 завершен. Промежуточные данные сохранены: %s (строк: %d)", interim_path, len(df))

        # ==========================================
        # ЭТАП 2: ПОЛУЧЕНИЕ ОПРЕДЕЛЕНИЙ
        # ==========================================
        logger.info("--- [ЭТАП 2/3] ПОЛУЧЕНИЕ И ПРОВЕРКА ОПРЕДЕЛЕНИЙ ---")
        df = pd.read_excel(interim_path)
        df_enriched = await enrich_with_definitions_async(df, model, config)

        validation_json_path = str(os.path.join(config["paths"]["validation"], config["files"]["extended_validation"]))
        df_enriched.to_json(validation_json_path, force_ascii=False, orient='records', indent=4)

        validation_glossary_path = str(
            os.path.join(config["paths"]["validation"], config["files"]["validation_glossary"]))
        mapping = {
            "abbreviations": "abbr_definitions",
            "terms": "term_definitions",
            "dropped_abbreviations": "dropped_abbr",
            "dropped_terms": "dropped_terms"
        }
        export_df_to_json(df_enriched, mapping, validation_glossary_path)
        logger.info("Этап 2 завершен. Валидационные файлы сохранены в папку: %s", config["paths"]["validation"])

        # ==========================================
        # ЭТАП 3: ФОРМИРОВАНИЕ ИТОГОВОГО ГЛОССАРИЯ
        # ==========================================
        logger.info("--- [ЭТАП 3/3] РАЗРЕШЕНИЕ КОНФЛИКТОВ И СБОРКА ГЛОССАРИЯ ---")
        df_val = pd.read_json(validation_glossary_path, typ="series")

        final_abbrs, conflict_abbrs = aggregate_definitions(df_val["abbreviations"])
        final_terms, conflict_terms = aggregate_definitions(df_val["terms"])

        logger.info("Найдено конфликтов: %d для аббревиатур, %d для терминов.", len(conflict_abbrs),
                    len(conflict_terms))

        resolved_abbrs, resolved_terms = await resolve_conflicts_async(conflict_abbrs, conflict_terms, model, config)
        logger.info("Конфликты успешно разрешены.")

        final_abbrs.update(resolved_abbrs)
        final_terms.update(resolved_terms)

        final_data = {
            "abbreviations": {k: [v] if isinstance(v, str) else v for k, v in final_abbrs.items()},
            "terms": {k: [v] if isinstance(v, str) else v for k, v in final_terms.items()}
        }

        processed_path = str(os.path.join(config["paths"]["processed"], config["files"]["processed_file"]))
        with open(processed_path, "w", encoding="utf-8") as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)

        logger.info("=== ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН ===")
        logger.info("Итоговый глоссарий сохранен: %s (Аббревиатур: %d, Терминов: %d)",
                    processed_path, len(final_abbrs), len(final_terms))

    except Exception as e:
        # exc_info=True запишет в лог всю простыню ошибки (Traceback)
        logger.critical("Произошла критическая ошибка во время выполнения пайплайна!", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
