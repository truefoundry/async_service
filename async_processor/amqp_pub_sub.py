from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from aio_pika import Message, connect_robust

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
        self._wait_time_seconds = config.wait_time_seconds
        self._connection = None
        self._channel = None
        self._queue = None

    async def _get_connect(self):
        if self._connection:
            return self._connection
        self._connection = await connect_robust(self._queue_url)
        return self._connection

    async def _get_channel(self):
        if self._channel:
            return self._channel
        await self._get_connect()
        self._channel = await self._connection.channel()
        return self._channel

    async def _get_queue(self):
        if self._queue:
            return self._queue
        await self._get_channel()
        self._queue = await self._channel.declare_queue(self._queue_name)
        return self._queue

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[str]]:
        message = None
        queue = await self._get_queue()
        try:
            message = await queue.get(fail=False, timeout=self._wait_time_seconds)
        except Exception as ex:
            raise InputMessageFetchFailure(f"Error fetch input message: {ex}") from ex
        if not message:
            yield None
            return
        try:
            yield message.body.decode()
        except Exception as ex:
            raise InputMessageFetchFailure(f"Error decoding input message body: {ex}") from ex
        finally:
            try:
                await message.ack()
            except Exception as ex:
                raise InputFetchAckFailure(
                    f"Error publishing input message: {ex}"
                ) from ex

    async def publish_input_message(
        self, serialized_input_message: bytes, request_id: str
    ):
        channel = await self._get_channel()
        await channel.default_exchange.publish(
            Message(body=serialized_input_message), routing_key=self._queue_name
        )


class AMQPOutput(Output):
    def __init__(self, config: AMQPOutputConfig):
        self._queue_url = config.queue_url
        self._queue_name = config.queue_name
        self._connection = None
        self._channel = None
        self._queue = None

    async def _get_connect(self):
        if self._connection:
            return self._connection
        self._connection = await connect_robust(self._queue_url)
        return self._connection

    async def _get_channel(self):
        if self._channel:
            return self._channel
        await self._get_connect()
        self._channel = await self._connection.channel()
        return self._channel

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: Optional[str]
    ):
        queue = await self._get_channel()
        await queue.default_exchange.publish(
            Message(body=serialized_output_message), routing_key=self._queue_name
        )
