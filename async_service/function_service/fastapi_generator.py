from async_service.function_service.utils import (
    create_pydantic_model_from_function_signature,
    get_functions_dict_with_input_signatures
)
from pydantic import BaseModel
from fastapi import FastAPI
from async_service import (
    InputMessage,
    Processor,
    WorkerConfig,
    InputConfig,
    OutputConfig,
    Output
)
import json
import uuid
from typing import Callable, Dict

INTERNAL_FUNCTION_NAME = "internal_func_name"


async def send_request_to_queue(
    request_id: str, input: BaseModel, output_publisher: Output
):
    
    my_dict = dict(input._iter(to_dict=False))
    my_dict[INTERNAL_FUNCTION_NAME] = input.__class__.__name__
    input_message = InputMessage(request_id=request_id, body=my_dict)
    
    await output_publisher.publish_output_message(
        request_id=request_id,
        serialized_output_message=json.dumps(input_message.dict()).encode(),
    )


def wrapper_func(func, name: str, output_publisher: Output):
    async def wrapper(input: create_pydantic_model_from_function_signature(func, name)):
        request_id = str(uuid.uuid4())
        await send_request_to_queue(request_id, input, output_publisher)
        return request_id

    return wrapper


def create_async_server_for_functions(
    funct_dict: dict[str, callable], output_config: OutputConfig
):
    app = FastAPI()
    app.add_api_route(
        "/function-schemas",
        lambda : get_functions_dict_with_input_signatures(funct_dict),
        methods=["GET"],
    )
    output_publisher = output_config.to_output()
    for name, func in funct_dict.items():
        app.add_api_route(
            f"/{name.lower()}",
            wrapper_func(func, name, output_publisher),
            methods=["POST"],
        )

    return app


def create_worker_for_functions(
    func_dict: Dict[str, Callable],
    input_config: InputConfig,
    output_config: OutputConfig,
):
    app = FastAPI()

    class FunctionProcessor(Processor):
        def process(self, input_message: InputMessage) -> int:
            body = input_message.body
            func_name = body[INTERNAL_FUNCTION_NAME]
            del body[INTERNAL_FUNCTION_NAME]

            func = func_dict[func_name]
            return func(**body)

    app = FunctionProcessor().build_app(
        worker_config=WorkerConfig(
            input_config=input_config,
            output_config=output_config,
        ),
    )
    return app
