import asyncio
import json
import re

import aiohttp
import yaml

from config import config
from src.utils.logger import PipelineLogger

logger = PipelineLogger.get_logger(__name__)

with open("config/prompts.yaml", "r", encoding="utf-8") as f:
    prompts = yaml.safe_load(f)


class AsyncAPIModelClient:
    """Асинхронный клиент для обращения к локальному серверу (llama.cpp server) батчами."""

    def __init__(
        self, url: str, endpoint: str, temperature: float, max_parallel: int
    ) -> None:
        """
        Args:
            url (str): URL-адрес API локального сервера.
            endpoint (str): Эндпоинт обращения к локальному серверу.
            temperature (float): Параметр температуры для генерации ответов модели.
            max_parallel (int): Максимальное количество параллельных запросов к серверу
                (ограничивается семафором).
        """

        self.url = url
        self.endpoint = endpoint
        self.temperature = temperature
        self.semaphore = asyncio.Semaphore(max_parallel)
        logger.info(
            f"[LLM] [INIT] Создан клиент LLM: url={url}, endpoint={endpoint}, "
            f"temperature={temperature}, max_parallel={max_parallel}"
        )

    async def generate_async(
        self, session: aiohttp.ClientSession, prompt: str, stage: str
    ) -> str:
        """Выполняет асинхронный запрос к LLM для генерации ответа.

        Внимание: параметр prompt теперь ожидает ПОЛНОСТЬЮ готовую строку
        (инструкции + текст) со стороны вызывающей функции.

        Args:
            session (aiohttp.ClientSession): Текущая асинхронная HTTP-сессия.
            prompt (str): Полностью сформированный текст пользовательского запроса.
            stage (str): Название текущего этапа обработки, используемое для
                извлечения системного промпта из конфигурации (например, 'role_prompt').

        Returns:
            str: Сгенерированный текстовый ответ от модели. В случае ошибки запроса
                или парсинга возвращает строку "[]".
        """

        messages = [
            {"role": "system", "content": prompts["llm"][stage]["role_prompt"]},
            {"role": "user", "content": prompt},
        ]

        payload = {
            "messages": messages,
            "max_tokens": 300,
            "temperature": self.temperature,
            "cache_prompt": True,
        }

        logger.info(
            f"[LLM] [REQUEST] Этап: {stage}, длина промпта: {len(prompt)} символов"
        )

        async with self.semaphore:
            try:
                endpoint = f"{self.url}/{self.endpoint}"

                async with session.post(endpoint, json=payload) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(
                            f"[LLM] [RESPONSE] [ERROR] HTTP {response.status} на этапе {stage}: {error_text}"
                        )
                        logger.debug(f"Сломанный промпт: {prompt[:200]}...")
                        return "[]"

                    res_json = await response.json()
                    content = res_json["choices"][0]["message"]["content"]
                    logger.info(
                        f"[LLM] [RESPONSE] Этап: {stage}, длина ответа: {len(content)} символов"
                    )
                    return res_json["choices"][0]["message"]["content"]

            except aiohttp.ClientError as e:
                logger.error(
                    f"[LLM] [REQUEST] [ERROR] Ошибка соединения с LLM ({self.url}) на этапе {stage}: {e}",
                    exc_info=True,
                )
                return "[]"
            except Exception as e:
                logger.error(
                    f"[LLM] [REQUEST] [ERROR] Неожиданная ошибка при запросе к LLM на этапе {stage}: {e}",
                    exc_info=True,
                )
                return "[]"


def get_llm_client():
    """Создает и возвращает настроенный экземпляр асинхронного клиента LLM.

    Returns:
        AsyncAPIModelClient: Готовый к использованию клиент, инициализированный
            параметрами (url, temperature) из глобальной конфигурации.
    """
    try:
        return AsyncAPIModelClient(
            url=config.LLM_API_URL,
            endpoint=config.LLM_API_ENDPOINT,
            temperature=0.0,
            max_parallel=8,
        )
    except Exception as e:
        logger.error(
            f"[LLM] [INIT] [ERROR] Не удалось создать клиент LLM: {e}",
            exc_info=True,
        )
        logger.error(f"Ошибка при получении экземпляра клиента LLM: {e}")


def parse_llm_definition_response(response_text: str) -> str | None:
    try:
        match = re.search(r"{.*?}", response_text, re.DOTALL)
        if not match:
            return None

        data = json.loads(match.group(0))
        exp = data.get("expansion") or data.get("definition")

        if data.get("has_definition") and exp:
            return str(exp).strip()
    except Exception as e:
        logger.error(f"[DEFINE] [PARSE] [ERROR] Ошибка парсинга определения: {e}")
        return None
    return None


def parse_llm_extraction_response(response_text: str) -> list[str]:
    if not response_text:
        return []
    try:
        match = re.search(r"[.*?]", response_text, re.DOTALL)
        if not match:
            return []

        data = json.loads(match.group(0))
        return [str(item).strip() for item in data if item]
    except Exception as e:
        logger.error(f"[EXTRACT] [PARSE] [ERROR] Ошибка парсинга извлечения: {e}")
        return []
