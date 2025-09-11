from datetime import datetime
import uuid

from pydantic import BaseModel, Field


class Message(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    msg: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
