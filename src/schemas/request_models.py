from pydantic import BaseModel, Field

class TextInput(BaseModel):
    text: str = Field(..., description="Текст для обработки на первом этапе пайплайна")
