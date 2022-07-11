from pydantic import BaseModel
from datetime import datetime


class EnabledMetadata(BaseModel):
    text: bool
    entities: bool
    labels: bool
    vector: bool


class RequestData(BaseModel):
    created: datetime
    sentences: bool
    paragraphs: bool
    resources: bool
    fields: bool
    entities: bool
    labels: bool
    metadata: EnabledMetadata
