import abc
from typing import Any, Optional, Union

import orjson
from fastapi import FastAPI

from async_processor.app import ProcessorApp
from async_processor.types import InputMessage, OutputMessage, WorkerConfig


class Processor(abc.ABC):
    def input_deserializer(
        self, serialized_input_message: Union[str, bytes]
    ) -> InputMessage:
        return InputMessage(**orjson.loads(serialized_input_message))

    def output_serializer(self, output_message: OutputMessage) -> bytes:
        return orjson.dumps(output_message.dict(), option=orjson.OPT_SERIALIZE_NUMPY)

    def init(self):
        return

    @abc.abstractmethod
    def process(self, input_message: InputMessage) -> Any:
        ...

    def build_app(self, worker_config: Optional[WorkerConfig] = None) -> FastAPI:
        return ProcessorApp(processor=self, worker_config=worker_config).app
