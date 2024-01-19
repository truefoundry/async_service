import asyncio
import codecs
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
from urllib.parse import urlparse

import aiohttp

from async_processor import (
    AsyncProcessor,
    InputMessageInterface,
    OutputMessage,
    ProcessStatus,
)
from async_processor.logger import logger
from async_processor.pydantic_v1 import BaseModel, BaseSettings, confloat, conint
from async_processor.sidecar.types import StreamBatch


class _StreamConfig(BaseModel):
    chunk_size: conint(ge=512, le=1024 * 512) = 1024 * 8


class _Settings(BaseSettings):
    destination_url: str
    request_timeout: confloat(gt=0) = 3.0
    stream_config: _StreamConfig = _StreamConfig()

    class Config:
        env_prefix = "TFY_ASYNC_PROCESSOR_SIDECAR_"


settings = _Settings()  # type: ignore
logger.info(repr(settings))


class _StreamBatcher:
    def __init__(self, stream_reader: aiohttp.StreamReader, config: _StreamConfig):
        self._stream_reader = stream_reader
        self._config = config
        self._batch = StreamBatch(data="")
        self._len = 0
        self._decoder = codecs.getincrementaldecoder("utf8")()
        self._fetcher_task = asyncio.create_task(self._fetcher())

    async def _fetcher(self):
        while True:
            if self._len >= self._config.chunk_size:
                await asyncio.sleep(0)
                continue
            data = await self._stream_reader.read(n=self._config.chunk_size)
            self._len += len(data)
            decoded_data = self._decoder.decode(data, self._stream_reader.at_eof())
            # print(decoded_data)
            # if not self._batch.data:
            #     print("slow client")

            self._batch.data += decoded_data

            if self._stream_reader.at_eof():
                self._batch.is_eof = True
                break

    def _reset_batch(self):
        self._batch = StreamBatch(data="")
        self._len = 0

    async def iter(self) -> AsyncIterator[StreamBatch]:
        while True:
            if not self._batch.data and not self._fetcher_task.done():
                await asyncio.sleep(0)
                continue

            if self._batch.data:
                data_to_yield = self._batch
                self._reset_batch()
                # NOTE: `_fetcher` will continue run while the caller of `iter` is publishing the `StreamBatch`.
                # the next iteration of this loop will not run, till the `data_to_yield` has been published via
                # output.
                yield data_to_yield

            if self._fetcher_task.done():
                if self._batch.data:
                    yield self._batch
                break

        if self._fetcher_task.exception():
            raise self._fetcher_task.exception()


def _resolve_url(input_message: InputMessageInterface) -> str:
    url_from_message = input_message.get_request_url_path()

    if not url_from_message:
        return settings.destination_url

    parsed_base_url = urlparse(settings.destination_url)
    parsed_url_from_message = urlparse(url_from_message)

    return parsed_url_from_message._replace(
        netloc=parsed_base_url.netloc,
        scheme=parsed_base_url.scheme,
    ).geturl()


class _SidecarProcessor(AsyncProcessor):
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

    @asynccontextmanager
    async def _request(self, input_message: InputMessageInterface) -> AsyncIterator:
        async with self._client_session.request(
            method=(input_message.get_request_method() or "POST").upper(),
            url=_resolve_url(input_message),
            headers=input_message.get_request_headers(),
            json=input_message.get_body(),
            timeout=settings.request_timeout,
        ) as response:
            yield response

    async def _no_stream(
        self, input_message: InputMessageInterface
    ) -> AsyncIterator[OutputMessage]:
        async with self._request(input_message) as response:
            status = ProcessStatus.SUCCESS if response.ok else ProcessStatus.FAILED
            yield OutputMessage(
                request_id=input_message.get_request_id(),
                status=status,
                body=await response.text(),
                status_code=str(response.status),
                content_type=response.headers.get("content-type"),
                input_message=input_message if status is ProcessStatus.FAILED else None,
            )

    # NOTE: this assumes that the response is utf8 decodable.
    async def _stream(
        self, input_message: InputMessageInterface
    ) -> AsyncIterator[OutputMessage]:
        async with self._request(input_message) as response:
            status = ProcessStatus.SUCCESS if response.ok else ProcessStatus.FAILED
            content_type = response.headers.get("content-type")

            if status is ProcessStatus.FAILED:
                yield OutputMessage(
                    request_id=input_message.get_request_id(),
                    status=status,
                    body=await response.text(),
                    status_code=str(response.status),
                    content_type=content_type,
                    input_message=input_message
                    if status is ProcessStatus.FAILED
                    else None,
                )
                return

            batcher = _StreamBatcher(
                stream_reader=response.content, config=settings.stream_config
            )
            async for batch in batcher.iter():
                # print(batch)
                yield OutputMessage(
                    request_id=input_message.get_request_id(),
                    status=status,
                    body=batch,
                    status_code=str(response.status),
                    content_type=content_type,
                )

    async def process(
        self, input_message: InputMessageInterface
    ) -> AsyncIterator[OutputMessage]:
        func = (
            self._stream if input_message.should_stream_response() else self._no_stream
        )
        async for output_message in func(input_message):
            yield output_message


app = _SidecarProcessor().build_app()
