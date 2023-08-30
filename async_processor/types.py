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


class OutputMessage(BaseModel):
    request_id: str
    status: ProcessStatus
    body: Optional[Any] = None
    error: Optional[str] = None

    class Config:
        use_enum_values = True


class InputMessageFetchFailure(Exception):
    ...


class InputFetchAckFailure(Exception):
    ...


class OutputMessageFetchTimeoutError(Exception):
    ...


class Input(abc.ABC):
    @asynccontextmanager
    @abc.abstractmethod
    async def get_input_message(
        self,
    ) -> AsyncIterator[Optional[Union[str, bytes]]]:
        ...

    @abc.abstractmethod
    async def publish_input_message(
        self, serialized_output_message: bytes, request_id: str
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
    root_subject: str
    consumer_name: constr(regex=r"^[a-z0-9\-]{1,32}$")
    visibility_timeout: confloat(ge=1)
    wait_time_seconds: confloat(ge=1) = 5

    def to_input(self) -> Input:
        from async_processor.nats_pub_sub import NATSInput

        return NATSInput(self)


class Output(abc.ABC):
    @abc.abstractmethod
    async def publish_output_message(
        self, serialized_output_message: bytes, request_id: str
    ):
        ...

    @abc.abstractmethod
    async def get_output_message(self, request_id: str) -> Optional[OutputMessage]:
        ...


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
    root_subject: str

    def to_output(self) -> Output:
        from async_processor.nats_pub_sub import NATSOutput

        return NATSOutput(self)


class WorkerConfig(BaseModel):
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


class AsyncOutputResponse(BaseModel):
    request_id: str

    class Config:
        allow_extra = True
