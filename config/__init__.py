import os
import re
import yaml
from dotenv import load_dotenv

load_dotenv()

def _interpolate(value):
    if isinstance(value, str):
        def replacer(match):
            var_name = match.group(1) or match.group(2)
            return os.getenv(var_name, match.group(0))

        pattern = r'\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)'
        return re.sub(pattern, replacer, value)
    elif isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_interpolate(item) for item in value]
    else:
        return value


def load_config():
    try:
        env = os.getenv("CONFIG")
    except KeyError:
        raise KeyError("Не установлена переменная CONFIG в .env")
    config_path = os.path.join(os.path.dirname(__file__), f"{env}.yaml")
    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)
    config_data = _interpolate(raw_config)

    class Config:
        def __init__(self, data):
            for key, value in data.items():
                if isinstance(value, dict):
                    setattr(self, key, Config(value))
                else:
                    setattr(self, key, value)

        @property
        def DATABASE_URL(self):
            db = self.database
            return f"postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.db}"

        @property
        def CELERY_BROKER_URL(self):
            rb = self.rabbitmq
            return f"amqp://{rb.user}:{rb.password}@{rb.host}:{rb.port}//"

        @property
        def LLM_API_URL(self):
            llm = self.llm
            return f"http://{llm.host}:{llm.port}"

        @property
        def BATCH_SIZE(self):
            pipeline = self.pipeline
            return pipeline.batch_size

    return Config(config_data)


config = load_config()