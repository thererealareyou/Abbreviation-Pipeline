import requests
import sqlite3
import yaml
import asyncio

from src.utils.logger import PipelineLogger
from main import main

def load_config():
    with open("config/settings.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

logger = PipelineLogger.get_logger(__name__)
config = load_config()

def get_connection():
    try:
        return sqlite3.connect(config["db"]["name"])
    except sqlite3.Error as e:
        logger.error(f"Ошибка при подключении к базе данных {config["db"]["name"]}: {e}")
        return e

def create_schema():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {config["db"]["table_name"]} (
                sentence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sentence_content TEXT NOT NULL UNIQUE,
                abbrs TEXT,
                terms TEXT
            )
        ''')

        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при создани схемы базы данных {config["db"]["name"]}: {e}")
        return e

def check_health():
    return requests.get(url='http://localhost:8000/health')

def put_data(data: str):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
                    INSERT OR IGNORE INTO {config["db"]["table_name"]}
                        (sentence_content, abbrs, terms)
                    VALUES (?, ?, ?)
                    """,
            (data, "Empty", "Empty"),
        )
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при добавлении элемента к базе данных {config["db"]["name"]}: {e}")
        return e

def launch():
    asyncio.run(main(stage_1=False,
                     stage_2=True,
                     stage_3=True,
                     stage_4=True))
