from typing import List

from pydantic import BaseModel


class TextsRequest(BaseModel):
    document_id: str
    texts: List[str]


class ResetRequest(BaseModel):
    code: str
