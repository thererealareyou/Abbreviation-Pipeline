from pydantic import BaseModel, Field

class SuccessResponse(BaseModel):
    status: str
    message: str

class HealthResponse(BaseModel):
    """Статус работы сервиса"""
    status: str = Field(..., description="Статус сервиса")