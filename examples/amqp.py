import asyncio
import json
import random
import uuid

import aio_pika

from async_processor import (
    AMQPInputConfig,
    AMQPOutputConfig,
    InputMessage,
    Processor,
    WorkerConfig,
)

# change this config
input_url = "amqp://guest:guest@localhost:5672/"
input_queue_name = "home1"
output_url = "amqp://guest:guest@localhost:5672/"
output_queue_name = "home2"


class MultiplicationProcessor(Processor):
    def process(self, input_message: InputMessage) -> int:
        body = input_message.body
        return body["x"] * body["y"]


app = MultiplicationProcessor().build_app(
    worker_config=WorkerConfig(
        input_config=AMQPInputConfig(url=input_url, queue_name=input_queue_name),
        output_config=AMQPOutputConfig(url=output_url, queue_name=output_queue_name),
    ),
)


async def send_request(url: str, routing_key: str):
    connection = await aio_pika.connect_robust(url)

    async with connection:
        request_id = str(uuid.uuid4())

        channel = await connection.channel()

        payload = json.dumps(
            {
                "request_id": request_id,
                "body": {"x": random.randint(1, 100), "y": random.randint(1, 100)},
            }
        )
        print(payload)
        await channel.default_exchange.publish(
            aio_pika.Message(payload.encode()), routing_key=routing_key
        )


async def test():
    for _ in range(100):
        await send_request(url=input_url, routing_key=input_queue_name)


if __name__ == "__main__":
    asyncio.run(test())
