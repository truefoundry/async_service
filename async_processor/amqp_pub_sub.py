from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
from urllib.parse import urlparse

from aio_pika import Message, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractExchange,
    AbstractQueue,
)
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


def parse_amqp_url(input_url):
    config = {}
    parsed_url = urlparse(input_url)
    # config['username'] = parsed_url.username
    # config['password'] = parsed_url.password
    config["host"] = parsed_url.hostname
    config["port"] = parsed_url.port or 5672
    config["virtual_host"] = parsed_url.path[1:] if parsed_url.path else "/"
    return config


class AMQPInput(Input):
    def __init__(self, config: AMQPInputConfig):
        self._config = config
        self._nc = None
        self._ch = None
        self._queue = None

    async def _validate_queue_exists(self):
        channel = await self._get_channel()
        try:
            self._queue = await channel.declare_queue(
                self._config.routing_key, passive=True
            )
        except ChannelNotFoundEntity as ex:
            raise Exception(
                f"Queue {self._config.routing_key!r} does not exist."
                " Please create the queue before running the async processor."
            ) from ex

    async def __aenter__(self):
        await self._validate_queue_exists()
        return self

    async def _get_connect(self) -> AbstractConnection:
        if self._nc:
            return self._nc

        if self._config.auth:
            auth = self._config.auth.dict()
            _pc = parse_amqp_url(self._config.url)
            self._nc = await connect_robust(
                host=_pc["host"],
                port=_pc["port"],
                virtualhost=_pc["virtual_host"],
                **auth,
            )
        else:
            self._nc = await connect_robust(self._config.url)
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
        self._queue = await channel.declare_queue(
            self._config.routing_key, passive=True
        )
        return self._queue

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._ch:
            try:
                await self._ch.close()
            except Exception:
                logger.exception("Failed to drain and close AMQP channel")
        if not self._nc:
            return
        try:
            await self._nc.close()
        except Exception:
            logger.exception("Failed to drain and close AMQP connection")

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[bytes]]:
        message = None
        queue = await self._get_queue()
        try:
            message = await queue.get(
                fail=False, timeout=self._config.wait_time_seconds
            )
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
            Message(body=serialized_input_message), routing_key=self._config.routing_key
        )


class AMQPOutput(Output):
    def __init__(self, config: AMQPOutputConfig):
        self._config = config
        self._exchange = None
        self._nc = None
        self._ch = None

    async def _get_connect(self) -> AbstractConnection:
        if self._nc:
            return self._nc
        if self._config.auth:
            auth = self._config.auth.dict()
            _pc = parse_amqp_url(self._config.url)
            self._nc = await connect_robust(
                host=_pc["host"],
                port=_pc["port"],
                virtualhost=_pc["virtual_host"],
                **auth,
            )
        else:
            self._nc = await connect_robust(self._config.url)
        return self._nc

    async def _get_channel(self) -> AbstractChannel:
        if self._ch:
            return self._ch
        connection = await self._get_connect()
        self._ch = await connection.channel()
        return self._ch

    async def __aenter__(self):
        await self._get_exchange()
        return self

    async def _get_exchange(self) -> AbstractExchange:
        if self._exchange:
            return self._exchange
        channel = await self._get_channel()
        try:
            # https://aio-pika.readthedocs.io/en/latest/apidoc.html#aio_pika.Channel.get_exchange
            # Keep ensure=True only if exchange_name is provided
            self._exchange = await channel.get_exchange(
                self._config.exchange_name,
                ensure=True if self._config.exchange_name else False,
            )
        except ChannelNotFoundEntity as ex:
            raise Exception(
                f"Exchange {self._config.exchange_name!r} does not exist."
                " Please create the exchange before running the async processor."
            ) from ex
        return self._exchange

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._ch:
            try:
                await self._ch.close()
            except Exception:
                logger.exception("Failed to drain and close AMQP channel")
        if not self._nc:
            return
        try:
            await self._nc.close()
        except Exception:
            logger.exception("Failed to drain and close AMQP connection")

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: Optional[str]
    ):
        exchange = await self._get_exchange()
        await exchange.publish(
            Message(body=serialized_output_message),
            routing_key=self._config.routing_key,
        )
