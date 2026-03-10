import logging
import sys
from pathlib import Path


class PipelineLogger:
    """Класс для настройки и получения логгера пайплайна."""

    @staticmethod
    def get_logger(name: str, log_file: str = "logs/pipeline.log", level: int = logging.INFO) -> logging.Logger:
        """Инициализирует и возвращает настроенный логгер.

        Создает логгер, который выводит сообщения в указанный файл и продублирует
        их в стандартный вывод (консоль). Автоматически создает директорию для логов.

        Args:
            name (str): Имя логгера (обычно передается `__name__` из модуля-вызывателя).
            log_file (str, optional): Путь к файлу, в который будут записываться логи.
                По умолчанию "logs/pipeline.log".
            level (int, optional): Базовый уровень логирования (например, `logging.INFO`
                или `logging.DEBUG`). По умолчанию `logging.INFO`.

        Returns:
            logging.Logger: Настроенный объект логгера из стандартной библиотеки.
        """
        logger = logging.getLogger(name)

        if logger.hasHandlers():
            return logger

        logger.setLevel(level)

        formatter = logging.Formatter(
            fmt='[%(asctime)s] - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger
