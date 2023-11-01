from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional

import orjson
from fastapi import Body, FastAPI, Request
from fastapi.responses import Response
from prometheus_client import CollectorRegistry, make_asgi_app, multiprocess

from async_processor.logger import logger
from async_processor.types import OutputMessage, ProcessStatus, WorkerConfig
from async_processor.worker import WorkerManager

if TYPE_CHECKING:
    from async_processor.processor import AsyncProcessorWrapper


def _json_serializer(obj: Any) -> bytes:
    return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY)


def _make_multiprocess_metrics_app():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return make_asgi_app(registry=registry)


_WORKER_CONFIG_ENV_VAR_NAME = "TFY_ASYNC_PROCESSOR_WORKER_CONFIG"


class ProcessorApp:
    def __init__(
        self,
        *,
        processor: AsyncProcessorWrapper,
        worker_config: Optional[WorkerConfig] = None,
    ):
        self._processor = processor
        self._worker_config = worker_config

        if self._worker_config is None:
            worker_config_json = os.getenv(_WORKER_CONFIG_ENV_VAR_NAME)
            if worker_config_json:
                self._worker_config = WorkerConfig(**orjson.loads(worker_config_json))

        self._app = FastAPI(
            lifespan=self._lifespan,
            docs_url="/",
            root_path=os.getenv("TFY_SERVICE_ROOT_PATH", ""),
        )
        self._app.get("/health")(self._healthy_route_handler)
        self._app.get("/ready")(self._ready_route_handler)
        self._app.post("/process")(self._process_route_handler)

        if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
            self._app.mount("/metrics", _make_multiprocess_metrics_app())
        else:
            self._app.mount("/metrics", make_asgi_app())

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        logger.info("Invoking the processor init method")
        await self._processor.init()
        logger.info("Processor init method execution completed")
        worker_manager_task = None
        stop_event = asyncio.Event()
        if self._worker_config:
            logger.info("Starting workers")
            worker_manager_task = WorkerManager(
                worker_config=self._worker_config,
                processor=self._processor,
                stop_event=stop_event,
            ).run_forever()
            logger.info("Started workers")
        else:
            logger.warning(
                "Processor runner config not passed. Processor runner will not start.\n"
                "Only `/process` API will be available."
            )
        yield
        logger.info("Shutting down workers")
        stop_event.set()

        try:
            if worker_manager_task:
                await worker_manager_task
        except Exception:
            logger.exception("Exception raised while stopping the workers.")

        if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
            multiprocess.mark_process_dead(os.getpid())

    def _ready_route_handler(self):
        return ""

    def _healthy_route_handler(self):
        return ""

    async def _process_route_handler(
        self,
        request: Request,
        # We need this ` _: dict = Body(...)` to
        # ensure that the user can put request body in the Swagger page
        _: dict = Body(...),  # noqa: B008
    ) -> Response:
        start = time.perf_counter()
        payload = await request.body()
        input_message = self._processor.input_deserializer(payload)
        output = await self._processor.process(input_message)
        time_taken_for_request = time.perf_counter() - start
        logger.debug(
            "Time taken to process request: %f seconds", time_taken_for_request
        )

        return Response(
            content=self._processor.output_serializer(
                OutputMessage(
                    request_id=input_message.get_request_id(),
                    body=output,
                    status=ProcessStatus.SUCCESS,
                )
            ),
        )

    @property
    def app(self) -> FastAPI:
        return self._app
