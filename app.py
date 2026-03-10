import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from src.schemas.response_models import HealthResponse
from src.routers.abbr_router import router
import hands

# Инициализация приложения
app = FastAPI(
    title="Abbreviation Extraction API",
    description="API для извлечения аббревиатур и терминов с использованием LLM пайплайна",
    version="1.0.0"
)

# Подключение роутера
app.include_router(router)

@app.on_event("startup")
async def startup_event():
    """
    При запуске приложения убеждаемся, что таблица в БД создана.
    Используем функцию из hands.py.
    """
    hands.create_schema()

@app.get("/health", tags=["System"])
async def health_check():
    try:
        hands.check_health()

        return HealthResponse(
            status="healthy"
        )
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )

if __name__ == "__main__":
    # Запуск сервера
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=True)
