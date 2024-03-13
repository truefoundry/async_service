from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from amqpstorm import Connection

from starlette.concurrency import run_in_threadpool

from async_processor.types import (
    Input,
    OutputMessageFetchTimeoutError,
    InputMessageFetchFailure,
    InputFetchAckFailure,
    Output,
    AMQPInputConfig,
    AMQPOutputConfig,
)


class AMQPInput(Input):

    def __init__(self, config: AMQPInputConfig):
        self._hostname = config.hostname
        self._port = config.port
        self._queue_name = config.queue_name
        self._connection = Connection(
            hostname=self._hostname,
            port=self._port,
            **(config.auth.dict() if config.auth else {}),
        ).channel()
        
        
    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[str]]:
        try:
            response = await run_in_threadpool(
                self._connection.basic.get,
                queue=self._queue_name
            )
            if not response:
                yield None
                return
            else:
                yield response.
        except Exception as ex:
            raise InputMessageFetchFailure() from ex
        try:
            response.ack()
        except Exception as ex:
            raise InputFetchAckFailure() from ex


    async def publish_input_message(
        self, serialized_input_message: bytes, request_id: str
    ):
        await run_in_threadpool(
            self._connection.basic.publish,
            exchange='',
            routing_key=self._queue_name,
            body=serialized_input_message.decode() if isinstance(serialized_input_message, bytes) else serialized_input_message,
        )


class AMQPOutput(Output):
    def __init__(self, config: AMQPOutputConfig):
        self._hostname = config.hostname
        self._port = config.port
        self._queue_name = config.queue_name

        self._connection = Connection(
            hostname=self._hostname,
            port=self._port,
            **(config.auth.dict() if config.auth else {})
        ).channel()

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: Optional[str]
    ):
        await run_in_threadpool(
            self._connection.basic.publish,
            routing_key=self._queue_name,
            body=serialized_output_message.decode() if isinstance(serialized_output_message, bytes) else serialized_output_message,
        )