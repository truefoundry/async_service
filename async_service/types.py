import abc
import enum
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, TypeVar, Union

from pydantic import BaseModel, constr


class ProcessStatus(str, enum.Enum):
    FAILED = "FAILED"
    SUCCESS = "SUCCESS"


InputMessageBody = TypeVar("InputMessageBody")
OutputMessageBody = TypeVar("OutputMessageBody")
SerializedInputMessage = TypeVar("SerializedInputMessage", bound=Union[str, bytes])
SerializedOutputMessage = TypeVar("SerializedOutputMessage", bound=Union[str, bytes])


class InputMessage(BaseModel):
    request_id: constr(min_length=1)
    body: InputMessageBody

    class Config:
        arbitrary_types_allowed = True


class OutputMessage(BaseModel):
    request_id: str
    status: ProcessStatus
    body: OutputMessageBody
    error: Optional[str] = None

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True


class InputMessageFetchFailure(Exception):
    ...


class InputFetchAckFailure(Exception):
    ...


class Input(abc.ABC):
    @asynccontextmanager
    @abc.abstractmethod
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[SerializedInputMessage]]:
        ...


class InputConfig(abc.ABC, BaseModel):
    @abc.abstractmethod
    def to_input(self) -> Input:
        ...


class AWSAccessKeyAuth(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: Optional[str] = None


class SQSInputConfig(InputConfig):
    queue_url: str
    region_name: Optional[str] = None

    auth: Optional[AWSAccessKeyAuth] = None

    visibility_timeout: int
    wait_time_seconds: float = 19

    def to_input(self) -> Input:
        from async_service.sqs_pub_sub import SQSInput

        return SQSInput(self)


class NATSInputConfig(InputConfig):
    nats_url: str
    root_subject: str
    consumer_name: str
    visibility_timeout: float
    wait_time_seconds: float = 5

    def to_input(self) -> Input:
        from async_service.nats_pub_sub import NATSInput

        return NATSInput(self)


class Output(abc.ABC):
    @abc.abstractmethod
    async def publish_output_message(
        self, serialized_output_message: SerializedOutputMessage, request_id: str
    ):
        ...


class OutputConfig(abc.ABC, BaseModel):
    @abc.abstractmethod
    def to_output(self) -> Output:
        ...


class SQSOutputConfig(OutputConfig):
    queue_url: str
    region_name: Optional[str] = None

    auth: Optional[AWSAccessKeyAuth] = None

    def to_output(self) -> Output:
        from async_service.sqs_pub_sub import SQSOutput

        return SQSOutput(self)


class NATSOutputConfig(OutputConfig):
    nats_url: str
    root_subject: str

    def to_output(self) -> Output:
        from async_service.nats_pub_sub import NATSOutput

        return NATSOutput(self)


class ProcessorRunnerConfig(BaseModel):
    input_config: Union[
        SQSInputConfig,
        NATSInputConfig,
        InputConfig,
    ]
    output_config: Union[
        SQSOutputConfig,
        NATSOutputConfig,
        OutputConfig,
    ]

    class Config:
        arbitrary_types_allowed = True
