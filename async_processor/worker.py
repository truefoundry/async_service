from __future__ import annotations

import asyncio
import os
import signal
import time
from typing import TYPE_CHECKING, Optional, Union

from async_processor.logger import logger
from async_processor.prometheus_metrics import (
    MESSAGE_INPUT_LATENCY,
    collect_input_message_fetch_metrics,
    collect_output_message_publish_metrics,
    collect_total_message_processing_metrics,
)
from async_processor.types import (
    InputMessage,
    OutputMessage,
    ProcessStatus,
    WorkerConfig,
)

if TYPE_CHECKING:
    from async_processor.processor import AsyncProcessorWrapper


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


class _Worker:
    def __init__(self, worker_config: WorkerConfig, processor: AsyncProcessorWrapper):
        self._input = worker_config.input_config.to_input()
        self._output = (
            worker_config.output_config.to_output()
            if worker_config.output_config
            else None
        )
        self._processor = processor

    async def _publish_response(
        self, serialized_output_message: bytes, request_id: str
    ):
        if self._output:
            with collect_output_message_publish_metrics():
                await self._output.publish_output_message(
                    serialized_output_message=serialized_output_message,
                    request_id=request_id,
                )
        else:
            logger.debug("Skipping publishing response as output config is not present")

    async def _handle_msg(
        self,
        serialized_input_message: Union[str, bytes],
        received_at_epoch_ns: int,
    ):
        serialized_output_message: Optional[bytes] = None
        input_message: Optional[InputMessage] = None
        with collect_total_message_processing_metrics():
            try:
                input_message = self._processor.input_deserializer(
                    serialized_input_message
                )
                result = await self._processor.process(input_message=input_message)
                output_message = OutputMessage(
                    status=ProcessStatus.SUCCESS,
                    request_id=input_message.request_id,
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
                        request_id=input_message.request_id,
                        error=str(ex),
                    )
                    serialized_output_message = self._processor.output_serializer(
                        output_message
                    )
                raise ex
            finally:
                if input_message and input_message.published_at_epoch_ns:
                    MESSAGE_INPUT_LATENCY.set(
                        received_at_epoch_ns - input_message.published_at_epoch_ns
                    )
                if serialized_output_message and input_message:
                    await self._publish_response(
                        serialized_output_message=serialized_output_message,
                        request_id=input_message.request_id,
                    )

    async def run(self, stop_event: asyncio.Event):
        # Streams should be bootstrapped separately
        # if hasattr(self._input, "initialize_stream"):
        #     await self._input.initialize_stream()
        # if hasattr(self._output, "initialize_stream"):
        #     await self._output.initialize_stream()
        logger.info("Polling messages")
        while not stop_event.is_set():
            try:
                with collect_input_message_fetch_metrics():
                    async with self._input.get_input_message() as serialized_input_message:
                        if not serialized_input_message:
                            continue
                        received_at_epoch_ns = time.time_ns()
                        await self._handle_msg(
                            serialized_input_message=serialized_input_message,
                            received_at_epoch_ns=received_at_epoch_ns,
                        )
            except Exception:
                logger.exception("error raised in the main worker loop")
        logger.info("Worker finished")
