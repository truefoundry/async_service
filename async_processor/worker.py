from __future__ import annotations

import asyncio
import os
import random
import signal
import string
import time
from contextlib import AsyncExitStack
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Union

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
    args: Tuple = None,
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


class _Worker:
    def __init__(self, worker_config: WorkerConfig, processor: AsyncProcessorWrapper):
        self._worker_config = worker_config
        self._processor = processor

    async def _handle_msg(
        self,
        serialized_input_message: Union[str, bytes],
        received_at_epoch_ns: int,
        output: Optional[Output],
    ):
        serialized_output_message: Optional[bytes] = None
        input_message: Optional[InputMessageInterface] = None
        exception: Optional[Exception] = None
        with collect_total_message_processing_metrics() as collector:
            try:
                input_message = self._processor.input_deserializer(
                    serialized_input_message
                )
                result = await self._processor.process(input_message=input_message)
                if isinstance(result, OutputMessage):
                    output_message = result
                else:
                    output_message = OutputMessage(
                        status=ProcessStatus.SUCCESS,
                        request_id=input_message.get_request_id(),
                        body=result,
                    )
                serialized_output_message = self._processor.output_serializer(
                    output_message
                )
            except Exception as ex:
                logger.exception("error raised while handling message")
                if input_message:
                    output_message = OutputMessage(
                        status=ProcessStatus.FAILED,
                        request_id=input_message.get_request_id(),
                        error=str(ex),
                        input_message=input_message,
                    )
                    serialized_output_message = self._processor.output_serializer(
                        output_message
                    )
                exception = ex
            else:
                collector.set_output_status(output_message.status)

            if input_message and input_message.get_published_at_epoch_ns():
                MESSAGE_INPUT_LATENCY.set(
                    received_at_epoch_ns - input_message.get_published_at_epoch_ns()
                )

            if output:
                if serialized_output_message and input_message:
                    await _publish_response(
                        serialized_output_message=serialized_output_message,
                        request_id=input_message.get_request_id(),
                        output=output,
                    )
            else:
                logger.debug(
                    "Skipping publishing response as output config is not present"
                )

            if exception:
                raise exception

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
