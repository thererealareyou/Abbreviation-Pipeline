import asyncio
import os
import yaml
import pandas as pd
import json

from src.utils.logger import PipelineLogger
from src.extraction.model_client import AsyncAPIModelClient
from src.extraction.pdf_parser import extract_sentences_from_folder
from src.dataset_builder.dataset_examiner import aggregate_definitions
from src.dataset_builder.variations_former import form_corresponging_table
from src.extraction.extraction_pipeline import (build_initial_dataframe_async,
                                                enrich_with_definitions_async,
                                                resolve_conflicts_async)
from src.utils.io_helpers import export_df_to_json


logger = PipelineLogger.get_logger(__name__)

def load_config():
    with open("config/settings.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

async def main(stage_1: bool,
               stage_2: bool,
               stage_3: bool,
               stage_4: bool):
    try:
        config = load_config()
        logger.debug("Конфигурация успешно загружена.")

        model = AsyncAPIModelClient(
            url=config["llm"]["api"]["url"],
            max_parallel=config["llm"]["api"]["max_parallel"],
            temperature=config["llm"]["api"]["temperature"]
        )
        logger.info(f"LLM клиент инициализирован (max_parallel={config["llm"]["api"]["max_parallel"]}).")

        # ==========================================
        # ЭТАП 1: ИЗВЛЕЧЕНИЕ
        # ==========================================
        if stage_1:
            logger.info("--- [ЭТАП 1/4] ИЗВЛЕЧЕНИЕ АББРЕВИАТУР И ТЕРМИНОВ ---")
            chunks = extract_sentences_from_folder(config["paths"]["raw_data"]["folder"])

            if not chunks:
                logger.warning("Не найдено ни одного чанка для обработки. Завершение работы.")
                return

            logger.info(f"Успешно извлечено {len(chunks)} чанков для обработки.")

            df = await build_initial_dataframe_async(chunks, model, config)

            interim_path = str(os.path.join(config["paths"]["interim_data"]["folder"],
                                            config["paths"]["interim_data"]["sentences_xlsx"]))
            df.to_excel(interim_path, columns=["chunk", "abbrs", "terms"], index=False)
            logger.info(f"Этап 1 завершен. Промежуточные данные сохранены: {interim_path} (строк: {len(df)})")

        # ==========================================
        # ЭТАП 2: ПОЛУЧЕНИЕ ОПРЕДЕЛЕНИЙ
        # ==========================================
        if stage_2:
            interim_path = str(os.path.join(config["paths"]["interim_data"]["folder"],
                                            config["paths"]["interim_data"]["sentences_xlsx"]))
            logger.info("--- [ЭТАП 2/4] ПОЛУЧЕНИЕ И ПРОВЕРКА ОПРЕДЕЛЕНИЙ ---")
            df = pd.read_excel(interim_path)
            df_enriched = await enrich_with_definitions_async(df, model, config)

            validation_json_path = str(os.path.join(config["paths"]["validation"]["folder"],
                                            config["paths"]["validation"]["extended_json"]))
            df_enriched.to_json(validation_json_path, force_ascii=False, orient='records', indent=4)

            validation_glossary_path = str(os.path.join(config["paths"]["validation"]["folder"],
                                            config["paths"]["validation"]["gloss_json"]))
            mapping = {
                "abbreviations": "abbr_definitions",
                "terms": "term_definitions",
                "dropped_abbreviations": "dropped_abbr",
                "dropped_terms": "dropped_terms"
            }

            export_df_to_json(df_enriched, mapping, validation_glossary_path)
            logger.info(f"Этап 2 завершен. Валидационные файлы сохранены в папку: {config["paths"]["validation"]}")

        # ==========================================
        # ЭТАП 3: ФОРМИРОВАНИЕ ИТОГОВОГО ГЛОССАРИЯ
        # ==========================================
        if stage_3:
            logger.info("--- [ЭТАП 3/4] РАЗРЕШЕНИЕ КОНФЛИКТОВ И СБОРКА ГЛОССАРИЯ ---")
            validation_glossary_path = str(os.path.join(config["paths"]["validation"]["folder"],
                                                        config["paths"]["validation"]["gloss_json"]))

            df_val = pd.read_json(validation_glossary_path, typ="series")

            final_abbrs, conflict_abbrs = aggregate_definitions(df_val["abbreviations"])
            final_terms, conflict_terms = aggregate_definitions(df_val["terms"])

            logger.info(f"Найдено конфликтов: {len(conflict_abbrs)} для аббревиатур, {len(conflict_terms)} для терминов.")

            resolved_abbrs, resolved_terms = await resolve_conflicts_async(conflict_abbrs, conflict_terms, model, config)
            logger.info("Конфликты успешно разрешены.")

            final_abbrs.update(resolved_abbrs)
            final_terms.update(resolved_terms)

            final_data = {
                "abbreviations": {k: [v] if isinstance(v, str) else v for k, v in final_abbrs.items()},
                "terms": {k: [v] if isinstance(v, str) else v for k, v in final_terms.items()}
            }

            processed_path = str(os.path.join(config["paths"]["processed"]["folder"],
                                              config["paths"]["processed"]["final_gloss_json"]))
            with open(processed_path, "w", encoding="utf-8") as f:
                json.dump(final_data, f, ensure_ascii=False, indent=4)

            logger.info(f"Итоговый глоссарий сохранен: {processed_path} (Аббревиатур: {len(final_abbrs)}, Терминов: {len(final_terms)}")

        # ==========================================
        # ЭТАП 4: ФОРМИРОВАНИЕ ГЛОССАРИЯ НАПИСАНИЯ
        # ==========================================
        if stage_4:
            logger.info("--- [ЭТАП 4/4] ФОРМИРОВАНИЕ ГЛОССАРИЯ НАПИСАНИЯ ---")
            glossary_path = str(os.path.join(config["paths"]["processed"]["folder"],
                                              config["paths"]["processed"]["final_gloss_json"]))
            variations_path = str(os.path.join(config["paths"]["processed"]["folder"],
                                              config["paths"]["processed"]["eng_to_ru_json"]))

            form_corresponging_table(glossary_path, 5, variations_path)
            logger.info("=== ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН ===")

    except Exception as e:
        logger.critical(f"Произошла критическая ошибка во время выполнения пайплайна: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main(stage_1=False,
                     stage_2=True,
                     stage_3=True,
                     stage_4=True))
