import abc
from typing import Optional

import orjson
from fastapi import FastAPI

from async_service.app import ProcessorApp
from async_service.types import (
    InputMessage,
    OutputMessage,
    OutputMessageBody,
    SerializedInputMessage,
    SerializedOutputMessage,
    WorkerConfig,
)


class Processor(abc.ABC):
    def input_deserializer(
        self, serialized_input_message: SerializedInputMessage
    ) -> InputMessage:
        return InputMessage(**orjson.loads(serialized_input_message))

    def output_serializer(
        self, output_message: OutputMessage
    ) -> SerializedOutputMessage:
        return orjson.dumps(output_message.dict(), option=orjson.OPT_SERIALIZE_NUMPY)

    def init(self):
        return

    @abc.abstractmethod
    def process(self, input_message: InputMessage) -> OutputMessageBody:
        ...

    def build_app(self, worker_config: Optional[WorkerConfig] = None) -> FastAPI:
        return ProcessorApp(processor=self, worker_config=worker_config).app
