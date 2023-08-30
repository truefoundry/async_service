import json
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from nats import connect
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js import JetStreamContext
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import BadRequestError

from async_processor.logger import logger
from async_processor.types import (
    Input,
    InputFetchAckFailure,
    InputMessageFetchFailure,
    NATSInputConfig,
    NATSOutputConfig,
    Output,
    OutputMessageFetchTimeoutError,
)


def _get_work_queue_subject_pattern(root_subject: str):
    return f"{root_subject}.>"


def _get_result_store_subject_pattern(root_subject: str):
    return f"{root_subject}.>"


def _get_work_queue_stream_config(root_subject: str) -> StreamConfig:
    return StreamConfig(
        name=f"{root_subject}-queue",
        subjects=[_get_work_queue_subject_pattern(root_subject)],
        retention=RetentionPolicy.WORK_QUEUE,
        storage=StorageType.FILE,
    )


def _get_result_store_stream_config(root_subject: str) -> StreamConfig:
    return StreamConfig(
        name=f"{root_subject}-result",
        subjects=[_get_result_store_subject_pattern(root_subject)],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        max_age=7 * 24 * 60 * 60,
        max_msgs_per_subject=1,
    )


class NATSInput(Input):
    def __init__(self, config: NATSInputConfig):
        self._nats_url = config.nats_url
        self._root_subject = config.root_subject
        self._ack_wait = config.visibility_timeout
        self._consumer_name = config.consumer_name
        self._wait_time_seconds = config.wait_time_seconds
        self._js = None
        self._psub = None

    async def initialize_stream(self):
        await _initialize_stream(
            jetstream=await self._get_js_client(),
            stream_config=_get_work_queue_stream_config(self._root_subject),
        )

    async def _get_js_client(self):
        if self._js:
            return self._js
        self._js = (await connect(self._nats_url)).jetstream(timeout=10)
        return self._js

    async def _get_psub(self):
        if self._psub:
            return self._psub
        jetstream = await self._get_js_client()
        self._psub = await jetstream.pull_subscribe(
            subject=_get_work_queue_subject_pattern(self._root_subject),
            durable=self._consumer_name,
            config=ConsumerConfig(
                ack_wait=self._ack_wait,
                durable_name=self._consumer_name,
            ),
        )
        return self._psub

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[bytes]]:
        psub = await self._get_psub()
        while True:
            try:
                msgs = await psub.fetch(1, timeout=self._wait_time_seconds)
            except NatsTimeoutError:
                logger.debug("No message in queue")
                continue
            except Exception as ex:
                raise InputMessageFetchFailure() from ex
            for msg in msgs:
                try:
                    yield msg.data
                finally:
                    try:
                        await msg.ack()
                    except Exception as ex:
                        raise InputFetchAckFailure() from ex
            break

    async def publish_input_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        jetstream = await self._get_js_client()
        await jetstream.publish(
            subject=f"{self._root_subject}.{request_id}",
            payload=serialized_output_message,
            timeout=5,
        )


class NATSOutput(Output):
    def __init__(self, config: NATSOutputConfig):
        self._nats_url = config.nats_url
        self._js = None
        self._root_subject = config.root_subject

    async def _get_js_client(self) -> JetStreamContext:
        if self._js:
            return self._js
        self._js = (await connect(self._nats_url)).jetstream(timeout=10)
        return self._js

    async def initialize_stream(self):
        jetstream = await self._get_js_client()
        await _initialize_stream(
            jetstream=jetstream,
            stream_config=_get_result_store_stream_config(self._root_subject),
        )

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        jetstream = await self._get_js_client()
        await jetstream.publish(
            subject=f"{self._root_subject}.{request_id}",
            payload=serialized_output_message,
            timeout=5,
        )

    async def get_output_message(self, request_id: str, timeout: float = 1.0) -> bytes:
        jetstream = await self._get_js_client()
        sub = await jetstream.subscribe(
            subject=f"{self._root_subject}.{request_id}",
            config=ConsumerConfig(ack_policy=AckPolicy.NONE),
        )
        try:
            msg = await sub.next_msg(timeout=timeout)
        except NatsTimeoutError as ex:
            raise OutputMessageFetchTimeoutError(
                f"No message received for request_id: {request_id}"
            ) from ex
        return msg.data


async def _initialize_stream(jetstream: JetStreamContext, stream_config: StreamConfig):
    try:
        await jetstream.add_stream(stream_config)
    except BadRequestError as ex:
        if ex.err_code == 10058:
            await jetstream.update_stream(stream_config)
        else:
            raise ex
