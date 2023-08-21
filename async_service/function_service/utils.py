import pydantic
import inspect
from typing import Any, Dict, Callable


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

    return pydantic.create_model(
        model_name,
        **params,
        **keyword_only_params,
        __base__=pydantic.BaseModel,
        __config__=config,
    )

def get_functions_dict_with_input_signatures(functions_dict: Dict[str, Callable]):
    return {
        name: create_pydantic_model_from_function_signature(func, name).schema()
        for name, func in functions_dict.items()
    }