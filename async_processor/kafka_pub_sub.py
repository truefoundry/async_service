from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from kafka import KafkaConsumer, KafkaProducer
from starlette.concurrency import run_in_threadpool

from async_processor.logger import logger
from async_processor.types import (
    Input,
    InputFetchAckFailure,
    InputMessageFetchFailure,
    KafkaInputConfig,
    KafkaOutputConfig,
    Output,
)


class KafkaInput(Input):
    def __init__(self, config: KafkaInputConfig):
        self._bootstrap_servers = config.bootstrap_servers
        self._topic_name = config.topic_name
        self._auth = config.auth
        self._consumer_group_id = config.consumer_group_id
        self.wait_time_seconds = config.wait_time_seconds
        self._consumer = KafkaConsumer(
            self._topic_name,
            bootstrap_servers=self._bootstrap_servers,
            group_id=config.consumer_group_id,
            enable_auto_commit=False,
            **(
                {
                    "sasl_plain_username": config.auth.username,
                    "sasl_plain_password": config.auth.password,
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                }
                if config.auth
                else {}
            ),
        )
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            **(
                {
                    "sasl_plain_username": config.auth.username,
                    "sasl_plain_password": config.auth.password,
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                }
                if config.auth
                else {}
            ),
        )

    @asynccontextmanager
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[str]]:
        try:
            consumer_map = await run_in_threadpool(
                self._consumer.poll,
                timeout_ms=self.wait_time_seconds * 1000,
                max_records=1,
            )
        except Exception as ex:
            raise InputMessageFetchFailure() from ex

        if len(consumer_map) == 0:
            yield None
            return

        for _, msgs in consumer_map.items():
            for msg in msgs:
                try:
                    yield msg.value
                finally:
                    try:
                        await run_in_threadpool(self._consumer.commit)
                    except Exception as ex:
                        raise InputFetchAckFailure() from ex

    async def publish_input_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        await run_in_threadpool(
            self._producer.send, topic=self._topic_name, value=serialized_output_message
        )


class KafkaOutput(Output):
    def __init__(self, config: KafkaOutputConfig):
        self._bootstrap_servers = config.bootstrap_servers
        self._topic_name = config.topic_name
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            **(
                {
                    "sasl_plain_username": config.auth.username,
                    "sasl_plain_password": config.auth.password,
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                }
                if config.auth
                else {}
            ),
        )

    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        await run_in_threadpool(
            self._producer.send, topic=self._topic_name, value=serialized_output_message
        )

    async def get_output_message(self, request_id: str) -> bytes:
        raise NotImplementedError
