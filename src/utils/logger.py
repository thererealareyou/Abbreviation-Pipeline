import json
import logging
import os
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

trace_id_var: ContextVar[str] = ContextVar("trace_id", default="system")


class JSONFormatter(logging.Formatter):
    """Форматирует лог в JSON для сборщиков типа ELK/Loki."""

    def format(self, record):
        log_record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "process_id": record.process,
            "thread_name": record.threadName,
            "trace_id": trace_id_var.get(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record, ensure_ascii=False)


class ReadableConsoleFormatter(logging.Formatter):
    """Красивый текстовый форматтер для локальной разработки."""

    def format(self, record):
        colors = {
            "DEBUG": "\033[36m",  # Циан
            "INFO": "\033[32m",  # Зеленый
            "WARNING": "\033[33m",  # Желтый
            "ERROR": "\033[31m",  # Красный
            "CRITICAL": "\033[41m",  # Красный фон
        }
        reset = "\033[0m"
        color = colors.get(record.levelname, "")

        timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        trace_id = trace_id_var.get()

        log_str = f"{timestamp} {color}[{record.levelname:<7}]{reset} [{trace_id}] {record.name}: {record.getMessage()}"

        if record.exc_info:
            log_str += f"\n{self.formatException(record.exc_info)}"
        return log_str


class PipelineLogger:
    @staticmethod
    def setup_logging(level: int = logging.INFO):
        """Централизованная настройка при старте приложения."""
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        if root_logger.hasHandlers():
            root_logger.handlers.clear()

        is_development = os.getenv("APP_ENV", "production") == "development"

        console_handler = logging.StreamHandler(sys.stdout)
        if is_development:
            console_handler.setFormatter(ReadableConsoleFormatter())
        else:
            console_handler.setFormatter(JSONFormatter())
        root_logger.addHandler(console_handler)

        log_path = Path("logs/app.log")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path, maxBytes=50 * 1024 * 1024, backupCount=10, encoding="utf-8"
        )
        file_handler.setFormatter(JSONFormatter())
        root_logger.addHandler(file_handler)

        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("arq").setLevel(logging.INFO)

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)
