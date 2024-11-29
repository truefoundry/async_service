import abc
import enum
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Union

from async_processor.pydantic_v1 import BaseModel, Extra, confloat, conint, constr


@enum.unique
class ProcessStatus(str, enum.Enum):
    FAILED = "FAILED"

    # If the user is returning a generator from process method, SUCCESS is no longer a terminal status.
    # we may have to re-think this.
    SUCCESS = "SUCCESS"


class InputMessageInterface(abc.ABC, BaseModel):
    @abc.abstractmethod
    def get_request_id(self) -> Optional[str]:
        ...

    @abc.abstractmethod
    def get_published_at_epoch_ns(self) -> Optional[int]:
        ...

    # TODO: this method is only here for sidecar
    # move this logic to sidecar module
    @abc.abstractmethod
    def get_body(self) -> Any:
        ...

    @abc.abstractmethod
    def should_stream_response(self) -> bool:
        ...

    @abc.abstractmethod
    def get_request_headers(self) -> Optional[Dict[str, Union[str, List[str]]]]:
        ...

    @abc.abstractmethod
    def get_request_url_path(self) -> Optional[str]:
        ...

    @abc.abstractmethod
    def get_request_method(self) -> Optional[str]:
        ...


class InputMessageV2(InputMessageInterface):
    tfy_request_id: Optional[constr(regex=r"^[a-zA-Z0-9\-]{1,36}$")] = None
    tfy_published_at_epoch_ns: Optional[int] = None
    tfy_stream_response: bool = False
    tfy_request_headers: Optional[Dict[str, Union[str, List[str]]]] = None
    tfy_request_url_path: Optional[str] = None
    tfy_request_method: Optional[str] = None

    class Config:
        extra = Extra.allow

    def get_request_id(self) -> Optional[str]:
        return self.tfy_request_id

    def get_published_at_epoch_ns(self) -> Optional[int]:
        return self.tfy_published_at_epoch_ns

    def get_body(self) -> Dict:
        body = self.dict()

        for field in self.__fields__:
            if field in body:
                del body[field]

        return body

    def should_stream_response(self) -> bool:
        return self.tfy_stream_response

    def get_request_headers(self) -> Optional[Dict[str, Union[str, List[str]]]]:
        return self.tfy_request_headers

    def get_request_url_path(self) -> Optional[str]:
        return self.tfy_request_url_path

    def get_request_method(self) -> Optional[str]:
        return self.tfy_request_method


class OutputMessage(BaseModel):
    request_id: Optional[str]
    status: ProcessStatus
    body: Optional[Any] = None
    error: Optional[str] = None
    input_message: Optional[InputMessageV2] = None

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


class AMQPInputConfig(InputConfig):
    type: constr(regex=r"^amqp$") = "amqp"
    url: constr(
        regex=r"^(?:amqp|amqps):\/\/(?:([^:/?#\s]+)(?::([^@/?#\s]+))?@)?([^/?#\s]+)(?::(\d+))?\/?([^?#\s]*)?(?:\?(.*))?$"
    )
    queue_name: str
    wait_time_seconds: conint(ge=1, le=20) = 5

    def to_input(self) -> Input:
        from async_processor.amqp_pub_sub import AMQPInput

        return AMQPInput(self)


class NATSInputConfig(InputConfig):
    type: constr(regex=r"^nats$") = "nats"

    nats_url: Union[str, List[str]]
    auth: Optional[NATSUserPasswordAuth] = None
    root_subject: constr(regex=r"^[a-zA-Z0-9][a-zA-Z0-9\-.]+[a-zA-Z0-9]$")
    consumer_name: constr(regex=r"^[a-zA-Z0-9][a-zA-Z0-9\-_]+[a-zA-Z0-9]$")
    wait_time_seconds: confloat(ge=5) = 5
    stream_name: str

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
        self, serialized_output_message: bytes, request_id: Optional[str]
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


class AMQPOutputConfig(OutputConfig):
    type: constr(regex=r"^amqp$") = "amqp"

    url: constr(
        regex=r"^(?:amqp|amqps):\/\/(?:([^:/?#\s]+)(?::([^@/?#\s]+))?@)?([^/?#\s]+)(?::(\d+))?\/?([^?#\s]*)?(?:\?(.*))?$"
    )
    routing_key: str
    exchange_name: str = ""

    def to_output(self) -> Output:
        from async_processor.amqp_pub_sub import AMQPOutput

        return AMQPOutput(self)


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
        AMQPInputConfig,
        InputConfig,
    ]
    output_config: Optional[
        Union[
            SQSOutputConfig,
            NATSOutputConfig,
            KafkaOutputConfig,
            CoreNATSOutputConfig,
            AMQPOutputConfig,
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
