from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional

import orjson
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CollectorRegistry, make_asgi_app, multiprocess

from async_service.logger import logger
from async_service.runner import ProcessorRunner
from async_service.types import InputMessage, OutputMessage, ProcessorRunnerConfig

if TYPE_CHECKING:
    from async_service.processor import Processor


def _json_serializer(obj: Any) -> bytes:
    return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY)


def _make_multiprocess_metrics_app():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return make_asgi_app(registry=registry)


_PROCESSOR_RUNNER_CONFIG_ENV_VAR_NAME = "PROCESSOR_RUNNER_CONFIG"


class ProcessorApp:
    def __init__(
        self,
        *,
        processor: Processor,
        processor_runner_config: Optional[ProcessorRunnerConfig] = None,
    ):
        self._processor = processor
        self._processor_runner_config = processor_runner_config

        if (
            self._processor_runner_config is None
            and _PROCESSOR_RUNNER_CONFIG_ENV_VAR_NAME in os.environ
        ):
            self._processor_runner_config = ProcessorRunnerConfig(
                **orjson.loads(os.environ[_PROCESSOR_RUNNER_CONFIG_ENV_VAR_NAME])
            )

        self._app = FastAPI(lifespan=self._lifespan, docs_url="/")
        self._app.get("/health")(self._healthy_route_handler)
        self._app.get("/ready")(self._ready_route_handler)
        self._app.post("/process")(self._process_route_handler)

        if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
            self._app.mount("/metrics", _make_multiprocess_metrics_app())
        else:
            self._app.mount("/metrics", make_asgi_app())

        self._runner = None

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        logger.info("Invoking the processor init method")
        self._processor.init()
        logger.info("Processor init method execution completed")
        if self._processor_runner_config:
            logger.info("Starting processor runner")
            loop = asyncio.get_running_loop()
            self._runner = ProcessorRunner(
                input_output_config=self._processor_runner_config,
                processor=self._processor,
            )
            loop.create_task(self._runner.run())
            logger.info("Started processor runner")
        else:
            logger.warning(
                "Processor runner config not passed. Processor runner will not start.\n"
                "Only `/process` API will be available."
            )
        yield
        multiprocess.mark_process_dead(os.getpid())

    def _ready_route_handler(self):
        return ""

    def _healthy_route_handler(self):
        if self._runner and not self._runner.healthy:
            raise HTTPException(status_code=500, detail="Worker not healthy")
        return ""

    def _process_route_handler(self, body: InputMessage) -> OutputMessage:
        start = time.perf_counter()
        output = self._processor.process(body)
        time_taken_for_request = time.perf_counter() - start
        logger.info("Time taken to process request: %f seconds", time_taken_for_request)

        return Response(
            content=self._processor.output_serializer(output),
        )

    @property
    def app(self) -> FastAPI:
        return self._app
