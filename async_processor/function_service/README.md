# Deploying Functions as Async Service

## Install
```console
pip install "async_processor[nats]"
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

Note: Currently only the following types are supported as arguments - `int`, `str`, `float`, `list`, `dict`, `None`. In the future we plan to decouple the serialization/deserialization from queue publish/subscribe to support arbitrary types

### Create the FastAPI applications
### `main.py`
```python
from async_processor import (
    WorkerConfig,
    FunctionAsyncExecutor,
    NATSInputConfig,
    NATSOutputConfig,
)
from sample_functions import func1, func2

# Define the function names and corresponding functions
functions = {"func_1": func1, "func_2": func2}

# Configure the deployment
async_func_deployment = FunctionAsyncExecutor(
    functions=functions,
    worker_config=WorkerConfig(
        input_config=NATSInputConfig(
            nats_url="<paste nats url here>",
            root_subject="<paste root subject for work queue>",
            consumer_name="<name of consumer>",
        ),
        output_config = NATSOutputConfig(
            nats_url="<paste nats url here>",
            root_subject="paste subject root for result queue",
        )
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
uvicorn --host 0.0.0.0 --port 8001 main:worker_app
```

### Send Request to the server_app

* Finally you can send request to your server_app at the deployed endpoint. (OpenAPI Docs are Auto-Generated)
* You can fetch the results from server_app at `/result/{request_id}` if the Output Config Queue supports fetching results per request_id. It is NOT supported for SQS but supported for NATS

To trigger the func_1 you can use the curl request
```
curl -X 'POST' \
  'http://0.0.0.0:8000/func_1' \
  -H 'Content-Type: application/json' \
  -d '{
  "first_name": "string",
  "last_name": "string"
}'
```

This request returns in a format: `{"request_id" : "<some req id>"}`.
You can now send a request to check the result for this request_id [Only applicable if Output is NATS]

```
curl 'http://0.0.0.0:8000/result/<paste your request id here>'
```
