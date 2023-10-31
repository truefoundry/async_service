import time
from contextlib import contextmanager

from prometheus_client import Counter, Gauge, Histogram

from async_processor.logger import logger
from async_processor.types import (
    InputFetchAckFailure,
    InputMessageFetchFailure,
    ProcessStatus,
)

_MESSAGES_IN_PROCESS = Gauge(
    "tfy_async_processor_messages_in_process",
    "Number of messages currently being processed by the worker",
    multiprocess_mode="livesum",
)

_MESSAGES_PROCESSED = Counter(
    "tfy_async_processor_messages_processed",
    "Number of messages processed by the worker",
    ["status"],
)

_MESSAGE_PROCESSING_TIME_MS = Gauge(
    "tfy_async_processor_processing_time_ms",
    "Time taken to process a single message in milliseconds",
    multiprocess_mode="livemax",
)

_MESSAGE_PROCESSING_TIME_MS_HISTOGRAM = Histogram(
    "tfy_async_processor_processing_time_histogram_ms",
    "Time taken to process a single message in milliseconds histogram",
    ["status"],
    # This is a catch all configuration.
    # Do not do performance testing using this.
    buckets=list(range(100, 550, 50))  # 9
    + list(range(750, 5250, 250))  # 18
    + list(range(5500, 10500, 500))  # 10
    + list(range(11000, 16000, 1000))  # 5
    + list(range(16000, 35000, 5000)),  # 4
)

MESSAGE_INPUT_LATENCY = Gauge(
    "tfy_async_processor_input_latency_ns",
    "Latency incurreed in the input queue",
    multiprocess_mode="livemax",
)

_OUTPUT_MESSAGE_PUBLISH_TIME_MS = Gauge(
    "tfy_async_processor_output_message_publish_time_ms",
    "Time taken to publish output message in milliseconds",
    multiprocess_mode="livemax",
)

_OUTPUT_MESSAGE_PUBLISH_FAILURES = Counter(
    "tfy_async_processor_output_message_publish_failures",
    "Number of times output message publish has failed",
)

_INPUT_MESSAGE_FETCH_FAILURES = Counter(
    "tfy_async_processor_input_message_fetch_failures",
    "Number of times input message fetching has failed",
)

_INPUT_MESSAGE_FETCH_ACK_FAILURES = Counter(
    "tfy_async_processor_input_message_fetch_ack_failures",
    "Number of times input message acking has failed",
)


def _perf_counter_ms() -> float:
    return time.perf_counter() * 1000


class collect_total_message_processing_metrics:
    def __init__(self):
        self._start: float = None  # type: ignore
        self._output_status = None

    def __enter__(self):
        _MESSAGES_IN_PROCESS.inc()
        self._start = _perf_counter_ms()
        return self

    def set_output_status(self, value):
        self._output_status = value

    def __exit__(self, exc_type, exc_value, exc_tb):
        end = _perf_counter_ms()
        _MESSAGES_IN_PROCESS.dec(1)

        if exc_value is not None:
            status = ProcessStatus.FAILED.value
        elif self._output_status is not None:
            status = self._output_status
        else:
            status = ProcessStatus.SUCCESS.value

        _MESSAGES_PROCESSED.labels(status=status).inc(1)
        message_processing_time = end - self._start
        logger.debug(
            "Time taken to process message: %f milliseconds",
            message_processing_time,
        )
        _MESSAGE_PROCESSING_TIME_MS.set(message_processing_time)
        _MESSAGE_PROCESSING_TIME_MS_HISTOGRAM.labels(status=status).observe(
            message_processing_time
        )


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
        logger.debug(
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
