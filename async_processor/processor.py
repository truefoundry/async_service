import abc
import inspect
from typing import Any, Callable, Optional, TypeVar, Union

import orjson
from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool

from async_processor.app import ProcessorApp
from async_processor.logger import logger
from async_processor.types import (
    InputMessageInterface,
    InputMessageV2,
    OutputMessage,
    WorkerConfig,
)

Result = TypeVar("Result")


async def _async_run(
    func_or_coroutine: Callable[..., Result], *args, **kwargs
) -> Result:
    if inspect.iscoroutinefunction(func_or_coroutine):
        return await func_or_coroutine(*args, **kwargs)
    return await run_in_threadpool(func_or_coroutine, *args, **kwargs)


class BaseProcessor:
    def input_deserializer(
        self, serialized_input_message: Union[str, bytes]
    ) -> InputMessageInterface:
        input_message = orjson.loads(serialized_input_message)
        if not isinstance(input_message, dict):
            raise ValueError(
                'Input message must be an object (E.g.: `{"a": "b"}`). '
                f"Expected dict, got {type(input_message)}"
            )
        logger.debug(f"Deserializing input message: {input_message!r}")

        ## This is really risky as this can fail if `input_message` is hitting any pydantic internal fields.
        ## MAKE THIS SAFE!!!
        return InputMessageV2(**input_message)

    def output_serializer(self, output_message: OutputMessage) -> bytes:
        logger.debug(f"Serializing output message: {output_message!r}")
        return orjson.dumps(output_message.dict(), option=orjson.OPT_SERIALIZE_NUMPY)


class Processor(abc.ABC, BaseProcessor):
    # TODO: init is not good enough
    # use a contextmanager here so that we can call it at startup and shutdown
    def init(self):
        return

    @abc.abstractmethod
    def process(self, input_message: InputMessageInterface) -> Any:
        ...

    def build_app(self, worker_config: Optional[WorkerConfig] = None) -> FastAPI:
        return ProcessorApp(
            processor=AsyncProcessorWrapper(self),
            worker_config=worker_config,
        ).app


class AsyncProcessor(abc.ABC, BaseProcessor):
    # TODO: init is not good enough
    # use a contextmanager here so that we can call it at startup and shutdown
    async def init(self):
        return

    @abc.abstractmethod
    async def process(self, input_message: InputMessageInterface) -> Any:
        ...

    def build_app(self, worker_config: Optional[WorkerConfig] = None) -> FastAPI:
        return ProcessorApp(
            processor=AsyncProcessorWrapper(self),
            worker_config=worker_config,
        ).app


class AsyncProcessorWrapper:
    def __init__(self, processor: Union[Processor, AsyncProcessor]):
        self._processor = processor

    def input_deserializer(
        self, serialized_input_message: Union[str, bytes]
    ) -> InputMessageInterface:
        return self._processor.input_deserializer(
            serialized_input_message=serialized_input_message
        )

    def output_serializer(self, output_message: OutputMessage) -> bytes:
        return self._processor.output_serializer(output_message=output_message)

    async def init(self):
        return await _async_run(self._processor.init)

    async def process(self, input_message: InputMessageInterface) -> Any:
        return await _async_run(self._processor.process, input_message=input_message)
