import uuid

from fastapi import FastAPI, Request

from src.backend.responses import UnicodeJSONResponse
from src.backend.routers import (danger, documents, global_dictionary, status,
                                 system)
from src.utils.logger import trace_id_var

app = FastAPI(
    title="Автоматический извлекатель доменных аббревиатур (АИДА)",
    default_response_class=UnicodeJSONResponse,
)


@app.middleware("http")
async def add_trace_id(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID", str(uuid.uuid4()))
    token = trace_id_var.set(trace_id)
    try:
        response = await call_next(request)
        response.headers["X-Trace-ID"] = trace_id
        return response
    finally:
        trace_id_var.reset(token)


app.include_router(system.router, tags=["System"])
app.include_router(documents.router, tags=["Documents"])
app.include_router(global_dictionary.router, tags=["Global Dictionary"])
app.include_router(status.router, tags=["Status"])
app.include_router(danger.router, tags=["Danger"])
