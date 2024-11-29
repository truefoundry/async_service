# Async Service

## Install
```console
pip install "async_processor[sqs]"
```

## Quick start

### Write the Processor
#### `app.py`
```python
from async_processor import (
    InputMessageInterface,
    Processor,
    WorkerConfig,
    SQSInputConfig,
    SQSOutputConfig,
)


class MultiplicationProcessor(Processor):
    def process(self, input_message: InputMessageInterface) -> int:
        body = input_message.get_body()
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
```console
gunicorn app:app --workers 1 --worker-class uvicorn.workers.UvicornWorker --bind 127.0.0.1:8000
```

Output:
```console
✦6 ❯ gunicorn app:app --workers 1 --worker-class uvicorn.workers.UvicornWorker --bind 127.0.0.1:8000
[2023-08-17 16:10:33 +0530] [78736] [INFO] Starting gunicorn 21.2.0
[2023-08-17 16:10:33 +0530] [78736] [INFO] Listening at: http://127.0.0.1:8000 (78736)
[2023-08-17 16:10:33 +0530] [78736] [INFO] Using worker: uvicorn.workers.UvicornWorker
[2023-08-17 16:10:33 +0530] [78738] [INFO] Booting worker with pid: 78738
[2023-08-17 16:10:33 +0530] [78738] [INFO] Started server process [78738]
[2023-08-17 16:10:33 +0530] [78738] [INFO] Waiting for application startup.
2023-08-17 16:10:33,764 - app - INFO - Invoking the processor init method
2023-08-17 16:10:33,765 - app - INFO - Processor init method execution completed
2023-08-17 16:10:33,765 - app - INFO - Starting processor runner
2023-08-17 16:10:34,461 - app - INFO - Started processor runner
2023-08-17 16:10:34,462 - app - INFO - Polling messages
[2023-08-17 16:10:34 +0530] [78738] [INFO] Application startup complete.
```

### Send a synchronus process request
```console
curl 'http://localhost:8000/process' -H 'Content-Type: application/json'  -d '{"request_id": "abc", "body": {"x": 1, "y": 2}}'
```

Output:
```console
❯ curl 'http://localhost:8000/process' -H 'Content-Type: application/json'  -d '{"request_id": "abc", "body": {"x": 1, "y": 2}}'
{"request_id":"abc","status":"SUCCESS","body":2,"error":null}
```

* A FastAPI documentation dashboard will be available on http://localhost

### Send an asynchronus process request
#### send_async_request.py
```python
import json
import uuid

from async_processor import InputMessageV2, OutputMessage, ProcessStatus
import boto3


def send_request(input_sqs_url: str, output_sqs_url: str):
    sqs = boto3.client("sqs")
    request_id = str(uuid.uuid4())

    sqs.send_message(
        QueueUrl=input_sqs_url,
        MessageBody=json.dumps(
            InputMessageV2(tfy_request_id=request_id, x=1, y=2).dict()
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
```console
python send_async_request.py
```

Output:
```console
❯ python send_async_request.py
request_id='46a4ebc6-afdb-46a0-8587-ba29abf0f0d4' status='SUCCESS' body=2 error=None
```
