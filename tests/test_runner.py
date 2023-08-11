import asyncio

import orjson

from async_service import InputMessage, OutputMessage, Processor, ProcessorRunnerConfig
from async_service.runner import ProcessorRunner
from tests.dummy_input_output import DummyInputConfig, DummyOutputConfig


class DummyProcessor(Processor):
    def process(self, input_message: InputMessage):
        return input_message


async def _test_processor_runner():
    messages = [
        InputMessage(request_id="1", body="1"),
        InputMessage(request_id="2", body="2"),
        InputMessage(request_id="3", body="3"),
    ]
    input_config = DummyInputConfig(messages=messages)
    output_config = DummyOutputConfig(results=[])
    runner = ProcessorRunner(
        processor=DummyProcessor(),
        processor_runner_config=ProcessorRunnerConfig(
            input_config=input_config, output_config=output_config
        ),
    )
    task = asyncio.create_task(runner.run())
    await asyncio.sleep(0.5)
    runner.stop()
    await asyncio.wait_for(task, timeout=1.0)

    assert len(messages) > 2
    assert len(messages) == len(output_config.results), len(output_config.results)

    for input_message, (serialized_output_message, request_id) in zip(
        messages, output_config.results
    ):
        assert input_message.request_id == request_id
        assert (
            OutputMessage(**orjson.loads(serialized_output_message)).body
            == input_message
        )


def test_processor_runner():
    asyncio.run(_test_processor_runner())
