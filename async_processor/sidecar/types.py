from typing import Optional

from async_processor.pydantic_v1 import BaseModel


class StreamBatch(BaseModel):
    data: str
    is_eof: Optional[bool] = None
    # Possibly add a batch number later
