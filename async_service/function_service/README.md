# Deploying Functions as Async Service

## Install
```console
pip install "async_service[sqs] @ git+https://github.com/truefoundry/async_service.git@main"
```

## Quick start

### Define the Sample Functions
### `sample_functions.py`
```python
from typing import Annotated
from pydantic import Field


def func1(first_name: str, last_name: str) -> str:
    return first_name + last_name


def func2(
    a: Annotated[int, Field(description="Enter first Number")] = 5,
    b: Annotated[int, Field(description="Enter Second Number")] = 10,
) -> int:
    return a + b
```

### Create the FastAPI applications
### `main.py`
```python
from async_service import (
    WorkerConfig,
    AsyncFunctionDeployment,
    SQSInputConfig,
    SQSOutputConfig,
)
from sample_functions import func1, func2

# Define the function names and corresponding functions
functions = {"func_1": func1, "func_2": func2}

# Configure the deployment
async_func_deployment = AsyncFunctionDeployment(
    functions=functions,
    worker_config=WorkerConfig(
        input_config=SQSInputConfig(
            queue_url="<Paste Input SQS Queue URL Here>", visibility_timeout=10
        ),
        output_config=SQSOutputConfig(
            queue_url="<Paste Output SQS URL Here>",
        ),
    ),
)

# Build and configure the applications
server_app = async_func_deployment.build_async_server_app()
worker_app = async_func_deployment.build_worker_app()

# These two apps can now be run on different ports or different machines.
```

### Run the applications locally ot deploy them
```
uvicorn --host 0.0.0.0 --port 8000 main:server_app
uvicorn --host 0.0.0.0 --port 8001 main:wroker_app
```

### Send Request to the server_app

* Finally you can send request to your server_app at the deployed endpoint. (OpenAPI Docs are Auto-Generated)
* You can fetch the results from server_app at `/result/{request_id}` if the Output Config Queue supports fetching results per request_id. It is NOT supported for SQS but supported for NATS
