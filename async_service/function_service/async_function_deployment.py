import os
from typing import Callable, Dict

from fastapi import FastAPI, HTTPException

from async_service import InputMessage, Processor, WorkerConfig
from async_service.function_service.utils import (
    INTERNAL_FUNCTION_NAME,
    async_wrapper_func,
    get_functions_dict_with_input_signatures,
)
from async_service.types import MessageProcessFailure, OutputMessageTimeoutError

FUNCTION_SCHEMA_ENDPOINT = "/function-schemas"
RESULT_ENDPOINT = "/result/{request_id}"


class AsyncFunctionDeployment:
    """
    A class for deploying and managing asynchronous functions with input and output configurations.

    Args:
        worker_config (WorkerConfig): Configuration for the worker behavior, including input and output config.
        functions (Dict[str, Callable]): A dictionary of function names and corresponding callable functions.
        init_function (Callable, optional): An initialization function called once before processing starts.

    Methods:
        build_async_server_app: Build and return an asynchronous server application for processing input.
        build_worker_app: Build and return an asynchronous worker application for executing functions.

    Usage Example:
        from async_service import (
            WorkerConfig,
            AsyncFunctionDeployment,
            SQSInputConfig,
            SQSOutputConfig,
        )
        from your_package import func1, func2

        # Define the function names and corresponding functions
        functions = {"func_1": func1, "func_2": func2}

        # Configure the deployment
        async_func_deployment = AsyncFunctionDeployment(
            functions=functions,
            worker_config=WorkerConfig(
                input_config=SQSInputConfig(
                    queue_url="<Paste SQS URL Here>",
                    visibility_timeout=10
                ),
                output_config=SQSOutputConfig(
                    queue_url="<Paste SQS URL Here>",
                )
            )
        )

        # Build and configure the applications
        server_app = async_func_deployment.build_async_server_app()
        worker_app = async_func_deployment.build_worker_app()

        # These two apps can now be run on different ports or different machines.
    """

    def __init__(
        self,
        worker_config: WorkerConfig,
        functions: Dict[str, Callable],
        init_function: Callable = None,
    ) -> None:
        if (
            INTERNAL_FUNCTION_NAME in functions
            or FUNCTION_SCHEMA_ENDPOINT.lstrip("/") in functions
            or RESULT_ENDPOINT.split("/")[1] in functions
        ):
            raise ValueError(
                f"Function names {INTERNAL_FUNCTION_NAME},  {FUNCTION_SCHEMA_ENDPOINT.lstrip('/')} and RESULT_ENDPOINT.split('/')[1] are reserved for internal use."
            )
        self.functions = functions
        self.worker_config = worker_config
        self.init_function = init_function

    def register_function(self, name: str, function: Callable) -> None:
        self.functions[name] = function

    def build_worker_app(self) -> FastAPI:
        app = FastAPI(root_path=os.getenv("TFY_SERVICE_ROOT_PATH"), docs_url="/")

        functions = self.functions
        init_function = self.init_function

        class FunctionProcessor(Processor):
            def init(self):
                if init_function:
                    init_function()

            def process(self, input_message: InputMessage) -> int:
                body = input_message.body
                func_name = body.pop(INTERNAL_FUNCTION_NAME, None)
                if func_name is None:
                    raise ValueError(
                        f"Input message does not contain {INTERNAL_FUNCTION_NAME} key."
                    )

                func = functions[func_name]
                return func(**body)

        app = FunctionProcessor().build_app(worker_config=self.worker_config)
        return app

    def build_async_server_app(self) -> FastAPI:
        app = FastAPI(root_path=os.getenv("TFY_SERVICE_ROOT_PATH"), docs_url="/")

        app.add_api_route(
            FUNCTION_SCHEMA_ENDPOINT,
            lambda: get_functions_dict_with_input_signatures(self.functions),
            methods=["GET"],
        )

        input_publisher = self.worker_config.input_config.to_input()
        output_subscriber = self.worker_config.output_config.to_output()

        if hasattr(output_subscriber, "get_output_message"):

            async def get_output(request_id: str):
                try:
                    return await output_subscriber.get_output_message(request_id)
                except OutputMessageTimeoutError as ex:
                    raise HTTPException(status_code=404, detail=str(ex))
                except MessageProcessFailure as ex:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Error processing the request: {str(ex)}",
                    )

            app.add_api_route(
                RESULT_ENDPOINT,
                get_output,
                methods=["GET"],
            )

        # check if all names are unique
        func_names_list = [name.lower() for name in list(self.functions.keys())]
        if len(func_names_list) != len(set(func_names_list)):
            raise ValueError(
                "Keys of functions dictionary (converted to lower case) must be unique."
            )

        for name, func in self.functions.items():
            app.add_api_route(
                f"/{name.lower()}",
                async_wrapper_func(func, name, input_publisher),
                methods=["POST"],
            )
        return app
