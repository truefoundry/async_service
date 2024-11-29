import inspect
import json
import re
import uuid
from typing import Any, Callable, Dict

from async_processor.pydantic_v1 import BaseModel, create_model
from async_processor.types import Input, InputMessageV2

INTERNAL_FUNCTION_NAME = "internal_func_name"


class AsyncOutputResponse(BaseModel):
    request_id: str

    class Config:
        allow_extra = True


def create_pydantic_model_from_function_signature(func, model_name: str):
    # https://github.com/pydantic/pydantic/issues/1391
    (
        args,
        _,
        varkw,
        defaults,
        kwonlyargs,
        kwonlydefaults,
        annotations,
    ) = inspect.getfullargspec(func)
    defaults = defaults or []
    args = args or []
    if len(args) > 0 and args[0] == "self":
        del args[0]

    non_default_args = len(args) - len(defaults)
    defaults = [
        ...,
    ] * non_default_args + list(defaults)

    keyword_only_params = {
        param: kwonlydefaults.get(param, Any) for param in kwonlyargs
    }
    params = {}
    for param, default in zip(args, defaults):
        params[param] = (annotations.get(param, Any), default)

    class Config:
        extra = "allow"

    # Allow extra params if there is a **kwargs parameter in the function signature
    config = Config if varkw else None

    return create_model(
        model_name,
        **params,
        **keyword_only_params,
        __base__=BaseModel,
        __config__=config,
    )


def get_functions_dict_with_input_signatures(functions_dict: Dict[str, Callable]):
    return {
        name: create_pydantic_model_from_function_signature(func, name).schema()
        for name, func in functions_dict.items()
    }


async def send_request_to_queue(
    request_id: str, input: BaseModel, input_publisher: Input
):
    my_dict = input.dict()
    my_dict[INTERNAL_FUNCTION_NAME] = input.__class__.__name__
    input_message = InputMessageV2(tfy_request_id=request_id, **my_dict)

    await input_publisher.publish_input_message(
        request_id=request_id,
        serialized_input_message=json.dumps(input_message.dict()).encode("utf-8"),
    )


def async_wrapper_func(func, name: str, output_publisher: Input):
    async def wrapper(input: create_pydantic_model_from_function_signature(func, name)):
        request_id = str(uuid.uuid4())
        await send_request_to_queue(request_id, input, output_publisher)
        return AsyncOutputResponse(request_id=request_id)

    return wrapper


def validate_function_name(input_string):
    pattern = r"^[^./\s]{1,30}$"
    if re.match(pattern, input_string):
        return True
    else:
        return False
