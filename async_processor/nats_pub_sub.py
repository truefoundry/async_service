import signal
from contextlib import asynccontextmanager
from functools import partial
from typing import AsyncIterator, Optional

from nats import NATS, connect
from nats.errors import FlushTimeoutError, OutboundBufferLimitError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig
from nats.js.errors import NotFoundError

from async_processor.logger import logger
from async_processor.types import (
    CoreNATSOutputConfig,
    Input,
    InputFetchAckFailure,
    InputMessageFetchFailure,
    NATSInputConfig,
    NATSOutputConfig,
    Output,
    OutputMessageFetchTimeoutError,
)

_MAX_RECONNECT_ATTEMPTS = -1
_MAX_OUTSTANDING_PINGS = 5
_PING_INTERVAL = 6


def _get_work_queue_subject_pattern(root_subject: str):
    return f"{root_subject}.>"


def _get_result_store_subject_pattern(root_subject: str):
    return f"{root_subject}.>"


async def _log_nats_connection_event(event: str):
    logger.warning("NATS connection event: %s", event)


class NATSInput(Input):
    def __init__(self, config: NATSInputConfig):
        self._config = config
        self._js = None
        self._psub = None
        self._nc = None

    async def __aenter__(self):
        await self._validate_consumer_exists()
        return self

    async def _get_nats_client(self) -> NATS:
        if self._nc:
            return self._nc

        auth = self._config.auth.dict() if self._config.auth else {}
        self._nc = await connect(
            self._config.nats_url,
            ping_interval=_PING_INTERVAL,
            max_outstanding_pings=_MAX_OUTSTANDING_PINGS,
            max_reconnect_attempts=_MAX_RECONNECT_ATTEMPTS,
            reconnected_cb=partial(_log_nats_connection_event, event="reconnected"),
            disconnected_cb=partial(_log_nats_connection_event, event="disconnected"),
            **auth,
        )
        return self._nc

    async def _get_js_client(self) -> JetStreamContext:
        if self._js:
            return self._js

        self._js = (await self._get_nats_client()).jetstream(timeout=10)
        return self._js

    async def _validate_consumer_exists(self):
        jetstream = await self._get_js_client()
        try:
            await jetstream.consumer_info(
                consumer=self._config.consumer_name,
                stream=self._config.stream_name,
            )
        except NotFoundError as ex:
            raise Exception(
                f"Consumer {self._config.consumer_name!r} does not exist."
                " Please create the consumer before running the async processor."
            ) from ex

    async def _get_psub(self) -> JetStreamContext.PullSubscription:
        if self._psub:
            return self._psub

        jetstream = await self._get_js_client()
        self._psub = await jetstream.pull_subscribe(
            subject=_get_work_queue_subject_pattern(self._config.root_subject),
            durable=self._config.consumer_name,
            config=ConsumerConfig(
                durable_name=self._config.consumer_name,
            ),
            stream=self._config.stream_name,
        )
        return self._psub

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[bytes]]:
        psub = await self._get_psub()
        msg = None
        try:
            # NOTE: default delta between expiry and timeout is 100ms.
            # This is hardcoded in the `fetch` public method and is too tight.
            # We are using the `_fetch_one` protected member directly to set custom
            # expiry.
            msg = await psub._fetch_one(
                timeout=self._config.wait_time_seconds,
                # This is in NS
                expires=int((self._config.wait_time_seconds - 0.8) * 1_000_000_000),
            )
        except NatsTimeoutError:
            logger.debug("No message in queue")
        except Exception as ex:
            raise InputMessageFetchFailure() from ex

        if not msg:
            yield None
            return

        try:
            yield msg.data
        finally:
            try:
                await msg.ack()
            except Exception as ex:
                raise InputFetchAckFailure() from ex

    async def publish_input_message(
        self, serialized_input_message: bytes, request_id: str
    ):
        jetstream = await self._get_js_client()
        await jetstream.publish(
            subject=f"{self._config.root_subject}.{request_id}",
            payload=serialized_input_message,
            timeout=5,
        )


class NATSOutput(Output):
    def __init__(self, config: NATSOutputConfig):
        self._config = config
        self._js = None
        self._nc = None

    async def _get_js_client(self) -> JetStreamContext:
        if self._js:
            return self._js

        auth = self._config.auth.dict() if self._config.auth else {}
        self._nc = await connect(
            self._config.nats_url,
            ping_interval=_PING_INTERVAL,
            max_outstanding_pings=_MAX_OUTSTANDING_PINGS,
            max_reconnect_attempts=_MAX_RECONNECT_ATTEMPTS,
            reconnected_cb=partial(_log_nats_connection_event, event="reconnected"),
            disconnected_cb=partial(_log_nats_connection_event, event="disconnected"),
            **auth,
        )
        self._js = self._nc.jetstream(timeout=10)
        return self._js

    async def __aexit__(self, exc_type, exc_value, traceback):
        if not self._nc:
            return
        try:
            await self._nc.close()
        except Exception:
            logger.exception("Failed to drain and close nats connection")

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        jetstream = await self._get_js_client()
        await jetstream.publish(
            subject=f"{self._config.root_subject}.{request_id}",
            payload=serialized_output_message,
            timeout=5,
        )

    async def get_output_message(self, request_id: str, timeout: float = 1.0) -> bytes:
        jetstream = await self._get_js_client()
        sub = await jetstream.subscribe(
            subject=f"{self._config.root_subject}.{request_id}",
            config=ConsumerConfig(ack_policy=AckPolicy.NONE),
        )
        try:
            msg = await sub.next_msg(timeout=timeout)
        except NatsTimeoutError as ex:
            raise OutputMessageFetchTimeoutError(
                f"No message received for request_id: {request_id}"
            ) from ex
        return msg.data


# We also have retries which will increment the counter
_MAX_CONSECUTIVE_FLUSH_TIMEOUT_COUNTS = 25


class CoreNATSOutput(Output):
    def __init__(self, config: CoreNATSOutputConfig):
        self._config = config
        self._nc = None
        self._consecutive_flush_timeout_counts = 0

    async def __aexit__(self, exc_type, exc_value, traceback):
        if not self._nc:
            return
        try:
            await self._nc.close()
        except Exception:
            logger.exception("Failed to drain and close nats connection")

    async def _get_nats_client(self) -> NATS:
        if self._nc:
            return self._nc

        auth = self._config.auth.dict() if self._config.auth else {}
        self._nc = await connect(
            self._config.nats_url,
            ping_interval=_PING_INTERVAL,
            max_outstanding_pings=_MAX_OUTSTANDING_PINGS,
            max_reconnect_attempts=_MAX_RECONNECT_ATTEMPTS,
            reconnected_cb=partial(_log_nats_connection_event, event="reconnected"),
            disconnected_cb=partial(_log_nats_connection_event, event="disconnected"),
            **auth,
        )
        return self._nc

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: Optional[str]
    ):
        nats = await self._get_nats_client()
        try:
            await nats.publish(
                subject=f"{self._config.root_subject}.{request_id}"
                if request_id
                else self._config.root_subject,
                payload=serialized_output_message,
            )
        except OutboundBufferLimitError as ex:
            # This can only happen if the nats connection is not in connected state for sometime
            # By default this buffer size is 2MB.
            # In case, the buffer gets full, we signal the process to terminate itself
            logger.exception("Fatal exception: OutboundBufferLimitError")
            signal.raise_signal(signal.SIGTERM)
            raise ex

        # Temporary measure to bubble up connection issues in our metrics
        try:
            await nats.flush(timeout=5)
        except FlushTimeoutError as ex:
            # In some cases, we notice that the >20% of the pods request starts
            # failing due to FlushTimeoutError. At the same time range, the other pods under the
            # same Deployment works without any issue.
            # Looking at nats connect and reconnect logs,
            # our hypothesis is that for whatever reason, the connection goes to a bad state.
            # In this case, we are tracking consecutive_flush_timeout and signal the process to terminate
            # if that count is above 25
            # After this, we mostly do not need sigterm for OutboundBufferLimitError
            self._consecutive_flush_timeout_counts += 1
            if (
                self._consecutive_flush_timeout_counts
                > _MAX_CONSECUTIVE_FLUSH_TIMEOUT_COUNTS
            ):
                logger.exception(
                    "Fatal exception: consecutive %d times FlushTimeoutError.",
                    self._consecutive_flush_timeout_counts,
                )
                signal.raise_signal(signal.SIGTERM)
            raise ex
        else:
            self._consecutive_flush_timeout_counts = 0
