import json
import yaml
import asyncio
import aiohttp
import pandas as pd
from colorama import init, Fore, Style
from pathlib import Path

with open("config/settings.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

init(autoreset=True)

class AsyncAPIModelClient:
    """Асинхронный клиент для обращения к локальному серверу (llama.cpp server) батчами.
    """

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
            {"role": "system", "content": config["llm"][stage]["role_prompt"]},
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
                    # Если ошибка (например, 400), читаем текст ошибки ДО вызова raise_for_status()
                    if response.status != 200:
                        error_text = await response.text()
                        print(f"\n[HTTP ОШИБКА {response.status} на этапе {stage}]")
                        print(f"Ответ сервера: {error_text}")
                        # Выводим первые 200 символов промпта, чтобы понять, какой текст сломал сервер
                        print(f"Сломанный промпт: {prompt[:200]}...")
                        return "[]"  # Возвращаем дефолт, чтобы не уронить весь батч

                    # Если всё ОК
                    res_json = await response.json()
                    return res_json["choices"][0]["message"]["content"]

            except Exception as e:
                print(f"[Критическая ошибка aiohttp]: {e}")
                return "[]"


def get_llm_client():
    """Создает и возвращает настроенный экземпляр асинхронного клиента LLM.

    Returns:
        AsyncAPIModelClient: Готовый к использованию клиент, инициализированный
            параметрами (url, temperature) из глобальной конфигурации.
    """

    return AsyncAPIModelClient(
        url=config["llm"]["api"]["url"],
        temperature=config["llm"]["temperature"],
        max_parallel=8
    )


def parse_llm_definition_response(response_text: str) -> str | None:
    """Парсит ответ LLM, извлекает expansion/definition, если has_definition == True. Возвращает строку или None.

    Args:
        response_text (str): Сырой текстовый ответ от LLM, ожидаемый в формате JSON
            (возможно, с markdown-разметкой), содержащий флаг `has_definition`
            и поле `expansion` или `definition`.

    Returns:
        str | None: Извлеченная очищенная строка с расшифровкой или определением,
            если флаг `has_definition` имеет значение True. В случае ошибки парсинга
            или отсутствия данных возвращает None.
    """

    try:
        clean_res = response_text.strip().replace('```json', '').replace('```', '')
        data = json.loads(clean_res)

        has_def = data.get("has_definition")
        exp = data.get("expansion") or data.get("definition")

        if has_def and exp:
            return exp.strip()
    except Exception:
        return None

    return None