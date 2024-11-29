from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, List, NamedTuple, Optional

import orjson

from async_processor import (
    Input,
    InputConfig,
    InputMessageInterface,
    Output,
    OutputConfig,
)


class DummyInputConfig(InputConfig):
    messages: List[InputMessageInterface]

    def to_input(self) -> Input:
        return DummyInput(self)


class DummyInput(Input):
    def __init__(self, config: DummyInputConfig):
        self._config = config

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[bytes]]:
        if len(self._config.messages) > 0:
            yield orjson.dumps(self._config.messages.pop(0).dict())
        else:
            yield None
        await asyncio.sleep(0)

    async def publish_input_message(
        self, serialized_input_message: bytes, request_id: str
    ):
        raise NotImplementedError


class _Result(NamedTuple):
    request_id: Optional[str]
    serialized_output_message: bytes


class DummyOutput(Output):
    def __init__(self, config: DummyOutputConfig):
        self._config = config

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: str | None
    ):
        self._config.results.append(
            _Result(
                serialized_output_message=serialized_output_message,
                request_id=request_id,
            )
        )
        await asyncio.sleep(0)


class DummyOutputConfig(OutputConfig):
    results: List[_Result]

    class Config:
        copy_on_model_validation = False

    def to_output(self) -> Output:
        return DummyOutput(self)
