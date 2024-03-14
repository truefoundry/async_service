from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from aio_pika import Message, connect_robust
from aio_pika.exceptions import AMQPError, QueueEmpty

from async_processor.types import (
    AMQPInputConfig,
    AMQPOutputConfig,
    Input,
    InputFetchAckFailure,
    InputMessageFetchFailure,
    Output,
)


class AMQPInput(Input):
    def __init__(self, config: AMQPInputConfig):
        self._queue_url = config.queue_url
        self._queue_name = config.queue_name
        self._connection = None

    async def _connect(self):
        self._connection = await connect_robust(self._queue_url)

    async def _disconnect(self):
        if self._connection is not None:
            await self._connection.close()

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[str]]:
        try:
            await self._connect()
            async with self._connection.channel() as channel:
                queue = await channel.declare_queue(self._queue_name)
                try:
                    message = await queue.get()
                    if message is None:
                        yield None
                    else:
                        async with message.process():
                            yield message.body.decode()
                except QueueEmpty:
                    yield None

        except AMQPError as ex:
            raise InputMessageFetchFailure(
                f"Error fetching input message: {ex}"
            ) from ex
        finally:
            await self._disconnect()

    async def publish_input_message(
        self, serialized_input_message: bytes, request_id: str
    ):
        try:
            await self._connect()
            async with self._connection.channel() as channel:
                await channel.declare_queue(self._queue_name)
                message_body = (
                    serialized_input_message
                    if isinstance(serialized_input_message, bytes)
                    else serialized_input_message
                )
                await channel.default_exchange.publish(
                    Message(body=message_body), routing_key=self._queue_name
                )
        except AMQPError as ex:
            raise InputFetchAckFailure(f"Error publishing input message: {ex}") from ex
        finally:
            await self._disconnect()


class AMQPOutput(Output):
    def __init__(self, config: AMQPOutputConfig):
        self._queue_url = config.queue_url
        self._queue_name = config.queue_name
        self._connection = None

    async def _connect(self):
        self._connection = await connect_robust(self._queue_url)

    async def _disconnect(self):
        if self._connection is not None:
            await self._connection.close()

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: Optional[str]
    ):
        try:
            await self._connect()
            async with self._connection.channel() as channel:
                await channel.declare_queue(self._queue_name)
                message_body = (
                    serialized_output_message
                    if isinstance(serialized_output_message, bytes)
                    else serialized_output_message
                )
                await channel.default_exchange.publish(
                    Message(body=message_body), routing_key=self._queue_name
                )
        except AMQPError as ex:
            raise InputFetchAckFailure(f"Error publishing output message: {ex}") from ex
        finally:
            await self._disconnect()
