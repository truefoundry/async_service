import asyncio
from typing import Optional

import aiohttp
from pydantic import BaseSettings, confloat

from async_processor import (
    AsyncProcessor,
    InputMessageInterface,
    OutputMessage,
    ProcessStatus,
)
from async_processor.logger import logger


class Settings(BaseSettings):
    destination_url: str
    request_timeout: confloat(gt=0) = 3.0

    class Config:
        env_prefix = "TFY_ASYNC_PROCESSOR_SIDECAR_"


settings = Settings()  # type: ignore


class SidecarProcessor(AsyncProcessor):
    def __init__(self):
        self._client_session: Optional[aiohttp.ClientSession] = None

    async def init(self):
        self._client_session = aiohttp.ClientSession()
        while True:
            try:
                async with self._client_session.head(
                    settings.destination_url,
                    timeout=settings.request_timeout,
                ):
                    ...
                break
            except Exception as ex:
                logger.warning(
                    "Cannot connect to %s retrying in 1 second. " "%s",
                    settings.destination_url,
                    str(ex),
                )
                await asyncio.sleep(1.0)

    async def process(self, input_message: InputMessageInterface) -> OutputMessage:
        async with self._client_session.post(
            settings.destination_url,
            json=input_message.get_body(),
            timeout=settings.request_timeout,
        ) as response:
            status = ProcessStatus.SUCCESS if response.ok else ProcessStatus.FAILED
            return OutputMessage(
                request_id=input_message.get_request_id(),
                status=status,
                body=await response.text(),
                status_code=str(response.status),
                content_type=response.headers["content-type"],
                input_message=input_message if status is ProcessStatus.FAILED else None,
            )


app = SidecarProcessor().build_app()
