from fastapi import FastAPI

from src.backend.responses import UnicodeJSONResponse
from src.backend.routers import system, documents, status, danger, global_dictionary

app = FastAPI(
    title="Автоматический извлекатель доменных аббревиатур (АИДА)",
    default_response_class=UnicodeJSONResponse
)

app.include_router(system.router, tags=["System"])
app.include_router(documents.router, tags=["Documents"])
app.include_router(global_dictionary.router, tags=["Global Dictionary"])
app.include_router(status.router, tags=["Status"])
app.include_router(danger.router, tags=["Danger"])
