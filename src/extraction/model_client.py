import json
import re
from config import config

import yaml
import asyncio
import aiohttp
from src.utils.logger import PipelineLogger


logger = PipelineLogger.get_logger(__name__)

with open("config/prompts.yaml", "r", encoding="utf-8") as f:
    prompts = yaml.safe_load(f)


class AsyncAPIModelClient:
    """Асинхронный клиент для обращения к локальному серверу (llama.cpp server) батчами."""
    def __init__(self, url: str, temperature: float, max_parallel: int) -> None:
        """
        Args:
            url (str): URL-адрес API локального сервера.
            temperature (float): Параметр температуры для генерации ответов модели.
            max_parallel (int): Максимальное количество параллельных запросов к серверу
                (ограничивается семафором).
        """

        self.url = url
        self.temperature = temperature
        self.semaphore = asyncio.Semaphore(max_parallel)

    async def generate_async(self, session: aiohttp.ClientSession, prompt: str, stage: str) -> str:
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
            {"role": "system", "content": prompts['llm'][stage]['role_prompt']},
            {"role": "user", "content": prompt}
        ]

        payload = {
            "messages": messages,
            "max_tokens": 300,
            "temperature": self.temperature,
            "cache_prompt": True
        }

        async with self.semaphore:
            try:
                endpoint = f"{self.url}/v1/chat/completions" if not self.url.endswith(
                    "/v1/chat/completions") else self.url

                async with session.post(endpoint, json=payload) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"\n[HTTP ОШИБКА {response.status} на этапе {stage}]. Ответ сервера: {error_text}")
                        logger.debug(f"Сломанный промпт: {prompt[:200]}...")
                        return "[]"

                    res_json = await response.json()
                    return res_json["choices"][0]["message"]["content"]

            except Exception as e:
                logger.error(f"[Критическая ошибка aiohttp]: {e}")
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
            temperature=0.0,
            max_parallel=8
        )
    except Exception as e:
        logger.error(f"Ошибка при получении экземпляра клиента LLM: {e}")


def parse_llm_definition_response(response_text: str) -> str | None:
    try:
        match = re.search(r"\{.*?\}", response_text, re.DOTALL)
        if not match:
            return None

        data = json.loads(match.group(0))
        exp = data.get("expansion") or data.get("definition")

        if data.get("has_definition") and exp:
            return str(exp).strip()
    except Exception as e:
        logger.error(f"[PARSE DEF] Ошибка парсинга: {e}")
        return None
    return None


def parse_llm_extraction_response(response_text: str) -> list[str]:
    if not response_text: return []
    try:
        match = re.search(r"\[.*?\]", response_text, re.DOTALL)
        if not match: return []

        data = json.loads(match.group(0))
        return [str(item).strip() for item in data if item]
    except Exception as e:
        logger.error(f"[PARSE EXTRACT] Ошибка: {e}")
        return []