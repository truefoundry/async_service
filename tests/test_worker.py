import asyncio
import uuid

import orjson

from async_processor import (
    AsyncProcessor,
    InputMessage,
    OutputMessage,
    Processor,
    WorkerConfig,
)
from async_processor.processor import AsyncProcessorWrapper
from async_processor.worker import WorkerManager
from tests.dummy_input_output import DummyInputConfig, DummyOutputConfig


async def _run_worker_manager(worker_manager: WorkerManager, stop_event: asyncio.Event):
    task = worker_manager.run_forever()
    await asyncio.sleep(0.1)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1.0)


async def _test_processor_runner_with_output_config():
    class DummyProcessor(Processor):
        def process(self, input_message: InputMessage):
            return input_message

    class AsyncDummyProcessor(AsyncProcessor):
        async def process(self, input_message: InputMessage):
            await asyncio.sleep(0.01)
            return input_message

    for P in (DummyProcessor, AsyncDummyProcessor):
        messages = [
            InputMessage(request_id=str(uuid.uuid4()), body="1"),
            InputMessage(request_id=uuid.uuid4().hex, body="2"),
            InputMessage(request_id="3", body="3"),
        ]
        input_config = DummyInputConfig(messages=messages)
        output_config = DummyOutputConfig(results=[])

        stop_event = asyncio.Event()
        worker_manager = WorkerManager(
            processor=AsyncProcessorWrapper(P()),
            worker_config=WorkerConfig(
                input_config=input_config,
                output_config=output_config,
                num_concurrent_workers=2,
            ),
            stop_event=stop_event,
        )
        await _run_worker_manager(worker_manager, stop_event)

        assert len(messages) > 2
        assert len(messages) == len(output_config.results), len(output_config.results)

        messages.sort(key=lambda x: x.request_id)
        output_config.results.sort(key=lambda x: x.request_id)

        for input_message, output_message in zip(messages, output_config.results):
            assert input_message.request_id == output_message.request_id
            assert (
                OutputMessage(
                    **orjson.loads(output_message.serialized_output_message)
                ).body
                == input_message
            )


async def _test_processor_runner_no_output_config():
    class DummyProcessor(Processor):
        def process(self, input_message: InputMessage):
            results[input_message.request_id].append(1)

    class AsyncDummyProcessor(AsyncProcessor):
        async def process(self, input_message: InputMessage):
            await asyncio.sleep(0.01)
            results[input_message.request_id].append(1)

    for P in (DummyProcessor, AsyncDummyProcessor):
        results = {"a": [], "b": [], "c": []}
        messages = [
            InputMessage(request_id="a", body={}),
            InputMessage(request_id="b", body={}),
            InputMessage(request_id="c", body={}),
        ]
        input_config = DummyInputConfig(messages=messages)

        stop_event = asyncio.Event()
        worker_manager = WorkerManager(
            processor=AsyncProcessorWrapper(P()),
            worker_config=WorkerConfig(
                input_config=input_config,
            ),
            stop_event=stop_event,
        )

        await _run_worker_manager(worker_manager, stop_event)

        assert len(messages) > 2
        for result in results.values():
            assert len(result) == 1


async def _test_input_message_in_output_message_if_processing_fails():
    class DummyProcessor(Processor):
        def process(self, input_message: InputMessage):
            raise Exception

    messages = [
        InputMessage(request_id="a", body={"foo": "bar"}),
        InputMessage(request_id="c", body={"a": "b"}),
    ]
    input_config = DummyInputConfig(messages=messages)
    output_config = DummyOutputConfig(results=[])

    stop_event = asyncio.Event()
    worker_manager = WorkerManager(
        processor=AsyncProcessorWrapper(DummyProcessor()),
        worker_config=WorkerConfig(
            input_config=input_config, output_config=output_config
        ),
        stop_event=stop_event,
    )
    await _run_worker_manager(worker_manager, stop_event)

    messages.sort(key=lambda x: x.request_id)
    output_config.results.sort(key=lambda x: x.request_id)

    for input_message, output_message in zip(messages, output_config.results):
        assert (
            OutputMessage(
                **orjson.loads(output_message.serialized_output_message)
            ).input_message
            == input_message
        )


async def _test_processor_runner():
    await _test_processor_runner_no_output_config()
    await _test_processor_runner_with_output_config()
    await _test_input_message_in_output_message_if_processing_fails()


def test_processor_runner():
    asyncio.run(_test_processor_runner())
