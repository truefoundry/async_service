import abc
from typing import Optional

import orjson
from fastapi import FastAPI

from async_service.app import ProcessorApp
from async_service.types import (
    InputMessage,
    OutputMessage,
    OutputMessageBody,
    ProcessorRunnerConfig,
    SerializedInputMessage,
    SerializedOutputMessage,
)


class Processor(abc.ABC):
    def input_deserializer(
        self, serialized_work_request: SerializedInputMessage
    ) -> InputMessage:
        return InputMessage(**orjson.loads(serialized_work_request))

    def output_serializer(
        self, work_response: OutputMessage
    ) -> SerializedOutputMessage:
        return orjson.dumps(work_response.dict(), option=orjson.OPT_SERIALIZE_NUMPY)

    def init(self):
        ...

    @abc.abstractmethod
    def process(self, input_message: InputMessage) -> OutputMessageBody:
        ...

    def build_app(
        self, processor_runner_config: Optional[ProcessorRunnerConfig] = None
    ) -> FastAPI:
        return ProcessorApp(
            processor=self, processor_runner_config=processor_runner_config
        ).app
