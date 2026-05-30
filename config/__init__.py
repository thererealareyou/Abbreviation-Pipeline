from httpx import URL
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    DATABASE_URL: str
    REDIS_URL: str
    LLM_API_URL: str
    LLM_HEALTH_ENDPOINT: str
    LLM_API_ENDPOINT: str

    @property
    def LLM_CHAT_URL(self) -> str:
        """Полный URL для отправки промптов"""
        return str(URL(self.LLM_API_URL).join(self.LLM_API_ENDPOINT))

    @property
    def LLM_HEALTH_URL(self) -> str:
        """Полный URL для проверки здоровья сервиса"""
        return str(URL(self.LLM_API_URL).join(self.LLM_HEALTH_ENDPOINT))

    BATCH_SIZE: int = 10
    MAX_WORKERS: int = 8


config = Settings()

DOCUMENT_PIPELINE_CONFIG = [
    {
        "stage": "extract",
        "batch_size": config.BATCH_SIZE,
        "tasks": ["bulk_extract_terms", "bulk_extract_abbrs"]
    },
    {
        "stage": "define",
        "batch_size": config.BATCH_SIZE,
        "tasks": [
            {"task": "bulk_define_terms", "type": "term"},
            {"task": "bulk_define_abbrs", "type": "abbr"}
        ]
    }
]

GLOBAL_DICT_PIPELINE_CONFIG = [
    {
        "stage": "resolve",
        "tasks": ["bulk_resolve_terms", "bulk_resolve_abbrs"]
    },
    {
        "stage": "transliterate",
        "tasks": ["bulk_transliterate_abbrs"]
    }
]
