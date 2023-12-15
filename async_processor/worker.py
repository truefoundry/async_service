from __future__ import annotations

import asyncio
import os
import random
import signal
import string
import time
import warnings
from contextlib import AsyncExitStack
from functools import wraps
from types import AsyncGeneratorType, GeneratorType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Optional,
    Tuple,
    Union,
)

from async_processor.logger import logger
from async_processor.prometheus_metrics import (
    MESSAGE_INPUT_LATENCY,
    collect_input_message_fetch_metrics,
    collect_output_message_publish_metrics,
    collect_total_message_processing_metrics,
)
from async_processor.types import (
    Input,
    InputMessageInterface,
    Output,
    OutputMessage,
    ProcessStatus,
    WorkerConfig,
)

if TYPE_CHECKING:
    from async_processor.processor import AsyncProcessorWrapper


async def _retry_with_exponential_backoff(
    coroutine: Callable,
    args: Optional[Tuple] = None,
    kwargs: Optional[Dict] = None,
    # The coroutine will always be awaited at least once.
    # regardless of num_retries.
    num_retries: int = 2,
    # base_ms needs to be more than 1. sub-ms does not work.
    base_ms: int = 5,
    jitter_ms: Optional[Tuple[int, int]] = (0, 100),
    max_wait_ms: float = 2000,
    retry_exceptions=(Exception,),
) -> Any:
    retry_count = 0
    exceptions_raised = []

    kwargs = kwargs or {}
    args = args or ()

    execution_name = getattr(coroutine, "__name__", "NO_NAME")
    execution_name = (
        f"{execution_name}-{''.join(random.choices(string.ascii_letters, k=4))}"
    )

    while True:
        try:
            return await coroutine(*args, **kwargs)
        except retry_exceptions as ex:
            exceptions_raised.append(ex)
            retry_count += 1

            if retry_count > num_retries:
                logger.warning(
                    "Execution=%s: Try=%d. Exception raised: %s. Will not retry",
                    execution_name,
                    retry_count,
                    str(ex),
                )
                break

            sleep_ms = base_ms**retry_count
            if jitter_ms:
                sleep_ms += random.uniform(*jitter_ms)
            sleep_ms = min(sleep_ms, max_wait_ms)

            logger.warning(
                "Execution=%s: Try=%d. Exception raised: %s. Retrying after %f ms.",
                execution_name,
                retry_count,
                str(ex),
                sleep_ms,
            )
            await asyncio.sleep(sleep_ms / 1000)

    logger.error(
        "Execution=%s: Failed to execute. Tried %d times. Raising Exception",
        execution_name,
        num_retries + 1,
    )
    raise Exception(exceptions_raised)


def _async_retry_with_exponential_backoff(
    num_retries: int = 2,
    # base_ms needs to be more than 1. sub-ms does not work.
    base_ms: int = 5,
    jitter_ms: Optional[Tuple[int, int]] = (0, 100),
    max_wait_ms: float = 2000,
    retry_exceptions=(Exception,),
):
    assert num_retries >= 0
    assert base_ms >= 2
    assert max_wait_ms >= base_ms
    if jitter_ms:
        assert jitter_ms[0] >= 0
        assert jitter_ms[1] >= 0

    def wrapper(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            return await _retry_with_exponential_backoff(
                coroutine=func,
                args=args,
                kwargs=kwargs,
                num_retries=num_retries,
                base_ms=base_ms,
                jitter_ms=jitter_ms,
                max_wait_ms=max_wait_ms,
                retry_exceptions=retry_exceptions,
            )

        return wrapped

    return wrapper


def _term_if_exception_raised(task: asyncio.Task):
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("Task %s failed", str(task))
        signal.raise_signal(signal.SIGTERM)


class WorkerManager:
    def __init__(
        self,
        worker_config: WorkerConfig,
        processor: AsyncProcessorWrapper,
        stop_event: asyncio.Event,
    ):
        self._processor = processor
        self._worker_config = worker_config
        self._stop_event = stop_event

    async def _start_workers(self):
        worker_tasks = []
        for _id in range(self._worker_config.num_concurrent_workers):
            worker_task = asyncio.create_task(
                _Worker(
                    worker_config=self._worker_config,
                    processor=self._processor,
                ).run(stop_event=self._stop_event),
                name=f"worker-{os.getpid()}-{_id}",
            )
            worker_task.add_done_callback(_term_if_exception_raised)
            worker_tasks.append(worker_task)

        await self._stop_event.wait()

        for worker_task in asyncio.as_completed(worker_tasks):
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Exception raised in worker while terminating")

    def run_forever(self) -> asyncio.Task:
        task = asyncio.create_task(
            self._start_workers(),
            name=f"workers-{os.getpid()}",
        )
        task.add_done_callback(_term_if_exception_raised)
        return task


@_async_retry_with_exponential_backoff(
    base_ms=10,
    retry_exceptions=(Exception,),
    num_retries=3,
    max_wait_ms=2000,
    jitter_ms=(0, 100),
)
async def _publish_response(
    serialized_output_message: bytes,
    request_id: Optional[str],
    output: Output,
):
    with collect_output_message_publish_metrics():
        await output.publish_output_message(
            serialized_output_message=serialized_output_message,
            request_id=request_id,
        )


async def _iter_and_warn(maybe_generator) -> AsyncIterator:
    if isinstance(maybe_generator, AsyncGeneratorType):
        warnings.warn(
            "Returning an AsyncGenerator from the AsyncProcessor.process method "
            "is an experimental feature, and behaviours may change later.",
            UserWarning,
            stacklevel=1,
        )
        async for obj in maybe_generator:
            yield obj
        return
    if isinstance(maybe_generator, GeneratorType):
        warnings.warn(
            "Returning an Generator from the Processor.process method "
            "is an experimental feature, and behaviours may change later.",
            UserWarning,
            stacklevel=1,
        )
        # NOTE: this may block the eventloop. Check it once.
        for obj in maybe_generator:
            yield obj
        return
    yield maybe_generator


def _prepare_failure_message_due_to_exception(
    input_message: InputMessageInterface, exception: Exception
) -> OutputMessage:
    return OutputMessage(
        status=ProcessStatus.FAILED,
        request_id=input_message.get_request_id(),
        error=str(exception),
        input_message=input_message,
    )


class _Worker:
    def __init__(self, worker_config: WorkerConfig, processor: AsyncProcessorWrapper):
        self._worker_config = worker_config
        self._processor = processor

    async def _process(
        self,
        serialized_input_message: Union[str, bytes],
        received_at_epoch_ns: int,
    ) -> AsyncIterator[Tuple[InputMessageInterface, OutputMessage]]:
        input_message = self._processor.input_deserializer(serialized_input_message)

        published_at_epoch_ns = input_message.get_published_at_epoch_ns()
        if published_at_epoch_ns:
            MESSAGE_INPUT_LATENCY.set(received_at_epoch_ns - published_at_epoch_ns)

        try:
            responses = await self._processor.process(input_message=input_message)
        except Exception as ex:
            logger.exception(
                "Failed to process message request_id=%s",
                input_message.get_request_id(),
            )
            yield input_message, _prepare_failure_message_due_to_exception(
                input_message=input_message, exception=ex
            )
            return

        responses = _iter_and_warn(responses)

        while True:
            try:
                resp = await responses.__anext__()
            except StopAsyncIteration:
                return
            except Exception as ex:
                logger.exception(
                    "Failed to process message request_id=%s",
                    input_message.get_request_id(),
                )
                yield input_message, _prepare_failure_message_due_to_exception(
                    input_message=input_message, exception=ex
                )
                return

            if isinstance(resp, OutputMessage):
                yield input_message, resp
                if resp.status is ProcessStatus.FAILED:
                    return
            else:
                yield input_message, OutputMessage(
                    status=ProcessStatus.SUCCESS,
                    request_id=input_message.get_request_id(),
                    body=resp,
                )

    async def _serialize_output_message(
        self, messages: AsyncIterator[Tuple[InputMessageInterface, OutputMessage]]
    ) -> AsyncIterator[Tuple[InputMessageInterface, OutputMessage, bytes]]:
        async for input_message, output_message in messages:
            serialized_output_message = self._processor.output_serializer(
                output_message
            )
            yield input_message, output_message, serialized_output_message

    async def _publish_output_message(
        self,
        messages: AsyncIterator[Tuple[InputMessageInterface, OutputMessage, bytes]],
        output: Output,
    ) -> AsyncIterator[Tuple[InputMessageInterface, OutputMessage]]:
        async for input_message, output_message, serialized_output_message in messages:
            await _publish_response(
                serialized_output_message=serialized_output_message,
                request_id=input_message.get_request_id(),
                output=output,
            )
            yield input_message, output_message

    async def _handle_msg(
        self,
        serialized_input_message: Union[str, bytes],
        received_at_epoch_ns: int,
        output: Optional[Output],
    ):
        with collect_total_message_processing_metrics() as collector:
            pipeline = self._process(
                serialized_input_message=serialized_input_message,
                received_at_epoch_ns=received_at_epoch_ns,
            )
            if output:
                pipeline = self._serialize_output_message(messages=pipeline)
                pipeline = self._publish_output_message(
                    messages=pipeline, output=output
                )
            else:
                logger.debug(
                    "Skipping publishing response as output config is not present"
                )

            async for _, output_message in pipeline:
                collector.set_output_status(output_message.status)

    async def _process_single_step(self, input_: Input, output: Optional[Output]):
        with collect_input_message_fetch_metrics():
            async with input_.get_input_message() as serialized_input_message:
                if not serialized_input_message:
                    return
                received_at_epoch_ns = time.time_ns()
                await self._handle_msg(
                    serialized_input_message=serialized_input_message,
                    received_at_epoch_ns=received_at_epoch_ns,
                    output=output,
                )

    async def run(self, stop_event: asyncio.Event):
        logger.info("Polling messages")
        async with AsyncExitStack() as stack:
            input_ = await stack.enter_async_context(
                self._worker_config.input_config.to_input()
            )
            output = None
            if self._worker_config.output_config:
                output = await stack.enter_async_context(
                    self._worker_config.output_config.to_output()
                )
            while not stop_event.is_set():
                try:
                    await self._process_single_step(input_=input_, output=output)
                except Exception:
                    logger.exception("error raised in the main worker loop")
        logger.info("Worker finished")
