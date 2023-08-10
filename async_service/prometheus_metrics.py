import time
from contextlib import contextmanager

from prometheus_client import Counter, Gauge

from async_service.logger import logger
from async_service.types import (
    InputFetchAckFailure,
    InputMessageFetchFailure,
    ProcessStatus,
)

_MESSAGES_IN_PROCESS = Gauge(
    "tfy_async_service_messages_in_process",
    "Number of messages currently being processed by the worker",
    multiprocess_mode="livesum",
)

_MESSAGES_PROCESSED = Counter(
    "tfy_async_service_messages_processed",
    "Number of messages processed by the worker",
    ["status"],
)

_MESSAGE_PROCESSING_TIME_MS = Gauge(
    "tfy_async_service_processing_time_ms",
    "Time taken to process a single message in milliseconds",
    multiprocess_mode="livemax",
)

_OUTPUT_MESSAGE_PUBLISH_TIME_MS = Gauge(
    "tfy_async_service_output_message_publish_time_ms",
    "Time taken to publish output message in milliseconds",
    multiprocess_mode="livemax",
)

_OUTPUT_MESSAGE_PUBLISH_FAILURES = Counter(
    "tfy_async_service_output_message_publish_failures",
    "Number of times output message publish has failed",
)

_INPUT_MESSAGE_FETCH_FAILURES = Counter(
    "tfy_async_service_input_message_fetch_failures",
    "Number of times input message fetching has failed",
)

_INPUT_MESSAGE_FETCH_ACK_FAILURES = Counter(
    "tfy_async_service_input_message_fetch_ack_failures",
    "Number of times input message acking has failed",
)


def _perf_counter_ms() -> float:
    return time.perf_counter() * 1000


@contextmanager
def collect_total_message_processing_metrics():
    _MESSAGES_IN_PROCESS.inc()
    start = _perf_counter_ms()
    try:
        yield
    except Exception as ex:
        _MESSAGES_PROCESSED.labels(status=ProcessStatus.FAILED.value).inc(1)
        raise ex
    else:
        _MESSAGES_PROCESSED.labels(status=ProcessStatus.SUCCESS.value).inc(1)
    finally:
        message_processing_time = _perf_counter_ms() - start
        logger.info(
            "Time taken to process message: %f milliseconds",
            message_processing_time,
        )
        _MESSAGE_PROCESSING_TIME_MS.set(message_processing_time)
        _MESSAGES_IN_PROCESS.dec(1)


@contextmanager
def collect_output_message_publish_metrics():
    start = _perf_counter_ms()
    try:
        yield
    except Exception as ex:
        _OUTPUT_MESSAGE_PUBLISH_FAILURES.inc(1)
        raise ex
    else:
        response_publish_time = _perf_counter_ms() - start
        logger.info(
            "Time taken to publish response back: %f milliseconds",
            response_publish_time,
        )
        _OUTPUT_MESSAGE_PUBLISH_TIME_MS.set(response_publish_time)


@contextmanager
def collect_input_message_fetch_metrics():
    try:
        yield
    except InputMessageFetchFailure as ex:
        _INPUT_MESSAGE_FETCH_FAILURES.inc(1)
        raise ex
    except InputFetchAckFailure as ex:
        _INPUT_MESSAGE_FETCH_ACK_FAILURES.inc(1)
        raise ex
