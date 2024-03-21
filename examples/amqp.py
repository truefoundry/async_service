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

from urllib.parse import urlparse


def parse_amqp_url(input_url):
    config = {}
    parsed_url = urlparse(input_url)
    # config['username'] = parsed_url.username
    # config['password'] = parsed_url.password
    config['host'] = parsed_url.hostname
    config['port'] = parsed_url.port or 5672
    config['virtual_host'] = parsed_url.path[1:] if parsed_url.path else "/"
    return config

# change this config
input_url = "amqp://localhost:5672/"
input_routing_key = "home1"
input_auth = {
     "login": "user1",
    "password": "password"
}
output_url = "amqp://localhost:5672/"
output_exchange_name = ""
output_routing_key = "home2"
output_auth = {
        "login": "user1",
        "password": "password"
    }


class MultiplicationProcessor(Processor):
    def process(self, input_message: InputMessage) -> int:
        body = input_message.body
        return body["x"] * body["y"]


app = MultiplicationProcessor().build_app(
    worker_config=WorkerConfig(
        input_config=AMQPInputConfig(
            url=input_url, 
            routing_key=input_routing_key,
            # auth=input_auth
        ),
        output_config=AMQPOutputConfig(
            url=output_url, 
            exchange_name=output_exchange_name,
            routing_key=output_routing_key,
            auth=output_auth
        ),
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
        await send_request(url=input_url, routing_key=input_routing_key)


if __name__ == "__main__":
    asyncio.run(test())
