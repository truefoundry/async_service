from __future__ import annotations

import signal
from typing import TYPE_CHECKING, Optional, Union

from starlette.concurrency import run_in_threadpool

from async_processor.logger import logger
from async_processor.prometheus_metrics import (
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
    from async_processor.processor import Processor


class Worker:
    def __init__(self, worker_config: WorkerConfig, processor: Processor):
        self._run = True
        self._input = worker_config.input_config.to_input()
        self._output = worker_config.output_config.to_output()
        self._processor = processor
        self._healthy = True
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

    def stop(self, *args, **kwargs):
        if logger:
            logger.info("Stopping worker")
        self._run = False

    @property
    def healthy(self) -> bool:
        return self._healthy

    async def _publish_response(
        self, serialized_output_message: bytes, request_id: str
    ):
        with collect_output_message_publish_metrics():
            await self._output.publish_output_message(
                serialized_output_message=serialized_output_message,
                request_id=request_id,
            )

    async def _handle_msg(
        self,
        serialized_input_message: Union[str, bytes],
    ):
        serialized_output_message: Optional[bytes] = None
        input_message: Optional[InputMessage] = None
        with collect_total_message_processing_metrics():
            try:
                input_message = self._processor.input_deserializer(
                    serialized_input_message
                )
                result = await run_in_threadpool(self._processor.process, input_message)
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
                if serialized_output_message and input_message:
                    await self._publish_response(
                        serialized_output_message=serialized_output_message,
                        request_id=input_message.request_id,
                    )

    async def run(self):
        try:
            # Streams should be bootstrapped separately
            # if hasattr(self._input, "initialize_stream"):
            #     await self._input.initialize_stream()
            # if hasattr(self._output, "initialize_stream"):
            #     await self._output.initialize_stream()
            logger.info("Polling messages")
            while True:
                if not self._run:
                    break
                try:
                    with collect_input_message_fetch_metrics():
                        async with self._input.get_input_message() as serialized_input_message:
                            if not serialized_input_message:
                                continue
                            await self._handle_msg(
                                serialized_input_message=serialized_input_message
                            )
                except Exception:
                    logger.exception("error raised in the main worker loop")
        except Exception as ex:
            logger.exception("worker failed")
            self._healthy = False
            raise ex
