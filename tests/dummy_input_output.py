from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, List, Optional, Tuple

import orjson

from async_service import (
    Input,
    InputConfig,
    InputMessage,
    Output,
    OutputConfig,
    SerializedInputMessage,
    SerializedOutputMessage,
)


class DummyInputConfig(InputConfig):
    messages: List[InputMessage]

    def to_input(self) -> Input:
        return DummyInput(self)


class DummyInput(Input):
    def __init__(self, config: DummyInputConfig):
        self._config = config

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[SerializedInputMessage]]:
        await asyncio.sleep(0.01)
        if len(self._config.messages) > 0:
            yield orjson.dumps(self._config.messages.pop(0).dict())
        else:
            yield None


class DummyOutput(Output):
    def __init__(self, config: DummyOutputConfig):
        self._config = config

    async def publish_output_message(
        self, serialized_output_message: SerializedOutputMessage, request_id: str
    ):
        await asyncio.sleep(0.01)
        self._config.results.append((serialized_output_message, request_id))


class DummyOutputConfig(OutputConfig):
    results: List[Tuple[SerializedOutputMessage, str]]

    class Config:
        copy_on_model_validation = False

    def to_output(self) -> Output:
        return DummyOutput(self)
