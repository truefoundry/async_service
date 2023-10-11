import abc
import enum
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, List, Optional, Union

from pydantic import BaseModel, confloat, conint, constr


@enum.unique
class ProcessStatus(str, enum.Enum):
    FAILED = "FAILED"
    SUCCESS = "SUCCESS"


class InputMessage(BaseModel):
    request_id: constr(regex=r"^[a-zA-Z0-9\-]{1,36}$")
    body: Any
    published_at_epoch_ns: Optional[int] = None


class OutputMessage(BaseModel):
    request_id: str
    status: ProcessStatus
    body: Optional[Any] = None
    error: Optional[str] = None

    # these are experimental fields
    status_code: Optional[str] = None
    content_type: Optional[str] = None

    class Config:
        use_enum_values = True


class InputMessageFetchFailure(Exception):
    ...


class InputFetchAckFailure(Exception):
    ...


class OutputMessageFetchTimeoutError(Exception):
    ...


class Input(abc.ABC):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return

    @asynccontextmanager
    @abc.abstractmethod
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[Union[str, bytes]]]:
        ...

    @abc.abstractmethod
    async def publish_input_message(
        self, serialized_input_message: bytes, request_id: str
    ):
        ...


class InputConfig(abc.ABC, BaseModel):
    class Config:
        frozen = True

    @abc.abstractmethod
    def to_input(self) -> Input:
        ...


class AWSAccessKeyAuth(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None


class KafkaSASLAuth(BaseModel):
    username: str
    password: str


class NATSUserPasswordAuth(BaseModel):
    user: str
    password: str


class SQSInputConfig(InputConfig):
    type: constr(regex=r"^sqs$") = "sqs"

    queue_url: str
    region_name: Optional[str] = None

    auth: Optional[AWSAccessKeyAuth] = None

    visibility_timeout: conint(gt=0, le=43200)
    wait_time_seconds: conint(ge=1, le=20) = 19

    def to_input(self) -> Input:
        from async_processor.sqs_pub_sub import SQSInput

        return SQSInput(self)


class NATSInputConfig(InputConfig):
    type: constr(regex=r"^nats$") = "nats"

    nats_url: Union[str, List[str]]
    auth: Optional[NATSUserPasswordAuth] = None
    root_subject: constr(regex=r"^[a-zA-Z0-9][a-zA-Z0-9\-.]+[a-zA-Z0-9]$")
    consumer_name: constr(regex=r"^[a-zA-Z0-9\-_]{1,32}$")
    wait_time_seconds: confloat(ge=5) = 5

    def to_input(self) -> Input:
        from async_processor.nats_pub_sub import NATSInput

        return NATSInput(self)


class KafkaInputConfig(InputConfig):
    type: constr(regex=r"^kafka$") = "kafka"

    bootstrap_servers: str
    topic_name: str
    consumer_group: str
    tls: bool = True
    auth: Optional[KafkaSASLAuth] = None
    wait_time_seconds: confloat(ge=1) = 5

    def to_input(self) -> Input:
        from async_processor.kafka_pub_sub import KafkaInput

        return KafkaInput(self)


class Output(abc.ABC):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return

    @abc.abstractmethod
    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        ...

    async def get_output_message(self, request_id: str) -> Optional[OutputMessage]:
        raise NotImplementedError


class OutputConfig(abc.ABC, BaseModel):
    class Config:
        frozen = True

    @abc.abstractmethod
    def to_output(self) -> Output:
        ...


class SQSOutputConfig(OutputConfig):
    type: constr(regex=r"^sqs$") = "sqs"

    queue_url: str
    region_name: Optional[str] = None

    auth: Optional[AWSAccessKeyAuth] = None

    def to_output(self) -> Output:
        from async_processor.sqs_pub_sub import SQSOutput

        return SQSOutput(self)


class NATSOutputConfig(OutputConfig):
    type: constr(regex=r"^nats$") = "nats"

    nats_url: Union[str, List[str]]
    root_subject: constr(regex=r"^[a-zA-Z0-9][a-zA-Z0-9\-.]+[a-zA-Z0-9]$")
    auth: Optional[NATSUserPasswordAuth] = None

    def to_output(self) -> Output:
        from async_processor.nats_pub_sub import NATSOutput

        return NATSOutput(self)


class CoreNATSOutputConfig(OutputConfig):
    type: constr(regex=r"^core-nats$") = "core-nats"

    nats_url: Union[str, List[str]]
    root_subject: constr(regex=r"^[a-zA-Z0-9][a-zA-Z0-9\-.]+[a-zA-Z0-9]$")
    auth: Optional[NATSUserPasswordAuth] = None

    def to_output(self) -> Output:
        from async_processor.nats_pub_sub import CoreNATSOutput

        return CoreNATSOutput(self)


class KafkaOutputConfig(OutputConfig):
    type: constr(regex=r"^kafka$") = "kafka"

    bootstrap_servers: str
    topic_name: str
    tls: bool = True
    auth: Optional[KafkaSASLAuth] = None

    def to_output(self) -> Output:
        from async_processor.kafka_pub_sub import KafkaOutput

        return KafkaOutput(self)


class WorkerConfig(BaseModel):
    input_config: Union[
        SQSInputConfig,
        NATSInputConfig,
        KafkaInputConfig,
        InputConfig,
    ]
    output_config: Optional[
        Union[
            SQSOutputConfig,
            NATSOutputConfig,
            KafkaOutputConfig,
            CoreNATSOutputConfig,
            OutputConfig,
        ]
    ] = None
    num_concurrent_workers: conint(ge=1, le=10) = 1

    class Config:
        arbitrary_types_allowed = True


class AsyncOutputResponse(BaseModel):
    request_id: str

    class Config:
        allow_extra = True
