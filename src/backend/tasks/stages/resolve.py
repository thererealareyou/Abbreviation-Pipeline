import asyncio
import json

import aiohttp
import yaml
from celery.utils.log import get_task_logger

from src.extraction.model_client import get_llm_client

logger = get_task_logger(__name__)

with open("config/prompts.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)


async def resolve_items(
    conflicts: dict[str, list[str]], item_type: str
) -> dict[str, str]:
    if not conflicts:
        logger.info(f"[RESOLVE] [START] {item_type}: конфликтов нет")
        return {}

    logger.info(
        f"[RESOLVE] [START] {item_type}: конфликтов {len(conflicts)}"
    )

    stage = f"resolve_{item_type}"
    instructions = config["llm"][stage]["instructions"]
    model = get_llm_client()
    keys = list(conflicts.keys())

    results: dict[str, str] = {}
    sem = asyncio.Semaphore(5)

    async def resolve_one(session, word):
        async with sem:
            try:
                prompt = instructions.replace("{item}", word).replace(
                    "{variations}", json.dumps(conflicts[word], ensure_ascii=False)
                )

                logger.info(
                    f"[RESOLVE] [LLM] [REQUEST] {item_type} '{word}' вариантов: {len(conflicts[word])}"
                )

                raw = await model.generate_async(session, prompt, stage=stage)

                if not raw:
                    raise ValueError("LLM вернула пустой запрос")

                clean = raw.strip().replace("```json", "").replace("```", "")
                data = json.loads(clean)
                final_val = data.get("resolved_definition", "").strip()

                results[word] = final_val if final_val else conflicts[word][0]

                logger.info(
                    f"[RESOLVE] [LLM] [RESPONSE] {item_type} '{word}' успешно разрешено"
                )

            except aiohttp.ClientError as e:
                logger.warning(
                    f"[RESOLVE] [LLM] [ERROR] Сетевая ошибка для '{word}': {e}. Использую первый вариант."
                )
                results[word] = conflicts[word][0]
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(
                    f"[RESOLVE] [LLM] [ERROR] Ошибка парсинга ответа для '{word}': {e}. Использую первый вариант."
                )
                results[word] = conflicts[word][0]
            except Exception as e:
                logger.error(
                    f"[RESOLVE] [LLM] [ERROR] Неожиданная ошибка для '{word}': {e}",
                    exc_info=True,
                )
                results[word] = conflicts[word][0]

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(*[resolve_one(session, w) for w in keys])

    logger.info(
        f"[RESOLVE] [FINISH] {item_type}: разрешено {len(results)} из {len(conflicts)}"
    )
    return results
