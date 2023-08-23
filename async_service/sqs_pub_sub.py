from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import boto3
from starlette.concurrency import run_in_threadpool

from async_service.logger import logger
from async_service.types import (
    Input,
    InputFetchAckFailure,
    InputMessageFetchFailure,
    Output,
    SQSInputConfig,
    SQSOutputConfig,
)


class SQSInput(Input):
    def __init__(self, config: SQSInputConfig):
        self._queue_url = config.queue_url
        self._visibility_timeout = config.visibility_timeout
        self._wait_time_seconds = config.wait_time_seconds
        self._sqs = boto3.client(
            "sqs",
            **(config.auth.dict() if config.auth else {}),
            region_name=config.region_name,
        )

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[str]]:
        while True:
            try:
                # Move this to its own thread and queue model later
                response = await run_in_threadpool(
                    self._sqs.receive_message,
                    QueueUrl=self._queue_url,
                    AttributeNames=["All"],
                    MaxNumberOfMessages=1,
                    MessageAttributeNames=["All"],
                    WaitTimeSeconds=self._wait_time_seconds,
                    VisibilityTimeout=self._visibility_timeout,
                )
            except Exception as ex:
                raise InputMessageFetchFailure() from ex
            if "Messages" not in response:
                logger.debug("No message in queue")
                continue
            for msg in response["Messages"]:
                receipt_handle = msg["ReceiptHandle"]
                try:
                    yield msg["Body"]
                finally:
                    try:
                        await run_in_threadpool(
                            self._sqs.delete_message,
                            QueueUrl=self._queue_url,
                            ReceiptHandle=receipt_handle,
                        )
                    except Exception as ex:
                        raise InputFetchAckFailure() from ex
            break

    async def publish_input_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        await run_in_threadpool(
            self._sqs.send_message,
            QueueUrl=self._queue_url,
            MessageBody=serialized_output_message.decode("utf-8")
            if isinstance(serialized_output_message, bytes)
            else serialized_output_message,
        )


class SQSOutput(Output):
    def __init__(self, config: SQSOutputConfig):
        self._queue_url = config.queue_url

        self._sqs = boto3.client(
            "sqs",
            **(config.auth.dict() if config.auth else {}),
            region_name=config.region_name,
        )

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        await run_in_threadpool(
            self._sqs.send_message,
            QueueUrl=self._queue_url,
            MessageBody=serialized_output_message.decode()
            if isinstance(serialized_output_message, bytes)
            else serialized_output_message,
        )

    async def get_output_message(request_id: str) -> bytes:
        raise NotImplementedError
