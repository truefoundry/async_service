from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from aio_pika import Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractQueue
from aio_pika.exceptions import ChannelNotFoundEntity, QueueEmpty

from async_processor.logger import logger
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
        self._url = config.url
        self._queue_name = config.queue_name
        self._wait_time_seconds = config.wait_time_seconds
        self._nc = None
        self._ch = None
        self._queue = None

    async def _validate_queue_exists(self):
        channel = await self._get_channel()
        try:
            self._queue = await channel.declare_queue(self._queue_name, passive=True)
        except ChannelNotFoundEntity as ex:
            raise Exception(
                f"Queue {self._queue_name!r} does not exist."
                " Please create the queue before running the async processor."
            ) from ex

    async def __aenter__(self):
        await self._validate_queue_exists()
        return self

    async def _get_connect(self) -> AbstractConnection:
        if self._nc:
            return self._nc
        self._nc = await connect_robust(self._url)
        return self._nc

    async def _get_channel(self) -> AbstractChannel:
        if self._ch:
            return self._ch
        connection = await self._get_connect()
        self._ch = await connection.channel()
        return self._ch

    async def _get_queue(self) -> AbstractQueue:
        if self._queue:
            return self._queue
        channel = await self._get_channel()
        self._queue = await channel.declare_queue(self._queue_name, passive=True)
        return self._queue

    async def __aexit__(self, exc_type, exc_value, traceback):
        if not self._nc:
            return
        try:
            await self._nc.close()
        except Exception:
            logger.exception("Failed to drain and close nats connection")

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[bytes]]:
        message = None
        queue = await self._get_queue()
        try:
            message = await queue.get(fail=False, timeout=self._wait_time_seconds)
        except QueueEmpty:
            logger.debug("No message in queue")
        except Exception as ex:
            raise InputMessageFetchFailure(f"Error fetch input message: {ex}") from ex
        if not message:
            yield None
            return
        try:
            yield message.body
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
        self._url = config.url
        self._queue_name = config.queue_name
        self._queue = None
        self._nc = None
        self._ch = None

    async def _validate_queue_exists(self):
        channel = await self._get_channel()
        try:
            self._queue = await channel.declare_queue(self._queue_name, passive=True)
        except ChannelNotFoundEntity as ex:
            raise Exception(
                f"Queue {self._queue_name!r} does not exist."
                " Please create the queue before running the async processor."
            ) from ex

    async def __aenter__(self):
        await self._validate_queue_exists()
        return self

    async def _get_connect(self) -> AbstractConnection:
        if self._nc:
            return self._nc
        self._nc = await connect_robust(self._url)
        return self._nc

    async def _get_channel(self) -> AbstractChannel:
        if self._ch:
            return self._ch
        connection = await self._get_connect()
        self._ch = await connection.channel()
        return self._ch

    async def _get_queue(self) -> AbstractQueue:
        if self._queue:
            return self._queue
        channel = await self._get_channel()
        self._queue = await channel.declare_queue(self._queue_name, passive=True)
        return self._queue

    async def __aexit__(self, exc_type, exc_value, traceback):
        if not self._nc:
            return
        try:
            await self._nc.close()
        except Exception:
            logger.exception("Failed to drain and close nats connection")

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: Optional[str]
    ):
        channel = await self._get_channel()
        await self._get_queue()
        await channel.default_exchange.publish(
            Message(body=serialized_output_message), routing_key=self._queue_name
        )
