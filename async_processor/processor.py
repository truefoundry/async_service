import abc
import inspect
from typing import Any, Callable, Optional, TypeVar, Union

import orjson
from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool

from async_processor.app import ProcessorApp
from async_processor.types import InputMessage, OutputMessage, WorkerConfig

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
    ) -> InputMessage:
        return InputMessage(**orjson.loads(serialized_input_message))

    def output_serializer(self, output_message: OutputMessage) -> bytes:
        return orjson.dumps(output_message.dict(), option=orjson.OPT_SERIALIZE_NUMPY)


class Processor(abc.ABC, BaseProcessor):
    # TODO: init is not good enough
    # use a contextmanager here so that we can call it at startup and shutdown
    def init(self):
        return

    @abc.abstractmethod
    def process(self, input_message: InputMessage) -> Any:
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
    async def process(self, input_message: InputMessage) -> Any:
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
    ) -> InputMessage:
        return self._processor.input_deserializer(
            serialized_input_message=serialized_input_message
        )

    def output_serializer(self, output_message: OutputMessage) -> bytes:
        return self._processor.output_serializer(output_message=output_message)

    async def init(self):
        return await _async_run(self._processor.init)

    async def process(self, input_message: InputMessage) -> Any:
        return await _async_run(self._processor.process, input_message=input_message)
