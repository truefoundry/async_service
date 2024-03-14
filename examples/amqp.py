from async_processor import (
    AMQPInputConfig,
    AMQPOutputConfig,
    InputMessage,
    Processor,
    WorkerConfig,
)


class MultiplicationProcessor(Processor):
    def process(self, input_message: InputMessage) -> int:
        body = input_message.body
        return body["x"] * body["y"]


app = MultiplicationProcessor().build_app(
    worker_config=WorkerConfig(
        input_config=AMQPInputConfig(
            queue_url="amqp://guest:guest@localhost:5672/", queue_name="home1"
        ),
        output_config=AMQPOutputConfig(
            queue_url="amqp://guest:guest@localhost:5672/", queue_name="home2"
        ),
    ),
)
