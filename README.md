# Async Service

## Install
```shell
pip install "async_service[sqs] @ git+https://github.com/truefoundry/async_service.git@main"
```

## Quick start

### Write the Processor
#### `app.py`
```python
from async_service import (
    InputMessage,
    Processor,
    WorkerConfig,
    SQSInputConfig,
    SQSOutputConfig,
)


class MultiplicationProcessor(Processor):
    def process(self, input_message: InputMessage) -> int:
        body = input_message.body
        return body["x"] * body["y"]


app = MultiplicationProcessor().build_app(
    worker_config=WorkerConfig(
        input_config=SQSInputConfig(
            queue_url="YOUR_INPUT_SQS_URL",
            visibility_timeout=2,
        ),
        output_config=SQSOutputConfig(queue_url="YOUR_OUTPUT_SQS_URL"),
    ),
)
```

### Run the app
```shell
gunicorn app:app --workers 1 --worker-class uvicorn.workers.UvicornWorker --bind 127.0.0.1:8000
```

### Send a synchronus process request
```shell
curl 'http://localhost:8000/process' -H 'Content-Type: application/json'  -d '{"request_id": "abc", "body": {"x": 1, "y": 2}}'
```
* A FastAPI documentation dashboard will be available on http://localhost

### Send an asynchronus process request
#### send_async_request.py
```python
import json
import uuid

from async_service import InputMessage, OutputMessage, ProcessStatus
import boto3


def send_request(input_sqs_url: str, output_sqs_url: str):
    sqs = boto3.client("sqs")
    request_id = str(uuid.uuid4())

    sqs.send_message(
        QueueUrl=input_sqs_url,
        MessageBody=json.dumps(
            InputMessage(request_id=request_id, body={"x": 1, "y": 2}).dict()
        ),
    )

    while True:
        response = sqs.receive_message(
            QueueUrl=output_sqs_url, MaxNumberOfMessages=1, WaitTimeSeconds=19
        )
        if "Messages" not in response:
            continue
        msg = response["Messages"][0]
        response = OutputMessage(**json.loads(msg["Body"]))

        if ProcessStatus[response.status] is not ProcessStatus.SUCCESS:
            raise Exception(f"processing failed: {response.error}")
        print(response)
        break

if __name__ == "__main__":
    send_request(input_sqs_url="YOUR_INPUT_SQS_URL", output_sqs_url="YOUR_OUTPUT_SQS_URL")
```

Run the above python script
```shell
python send_async_request.py
```
