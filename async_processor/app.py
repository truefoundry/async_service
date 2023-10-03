from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional
from warnings import warn

import orjson
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CollectorRegistry, make_asgi_app, multiprocess

from async_processor.logger import logger
from async_processor.types import (
    InputMessage,
    OutputMessage,
    ProcessStatus,
    WorkerConfig,
)
from async_processor.worker import Worker

if TYPE_CHECKING:
    from async_processor.processor import Processor


def _json_serializer(obj: Any) -> bytes:
    return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY)


def _make_multiprocess_metrics_app():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return make_asgi_app(registry=registry)


_WORKER_CONFIG_ENV_VAR_NAME_OLD = "TFY_WORKER_CONFIG"  # We will deprecate this.
_WORKER_CONFIG_ENV_VAR_NAME = "TFY_ASYNC_PROCESSOR_WORKER_CONFIG"


class ProcessorApp:
    def __init__(
        self,
        *,
        processor: Processor,
        worker_config: Optional[WorkerConfig] = None,
    ):
        self._processor = processor
        self._worker_config = worker_config

        if self._worker_config is None:
            for env_var_name in (
                _WORKER_CONFIG_ENV_VAR_NAME,
                _WORKER_CONFIG_ENV_VAR_NAME_OLD,
            ):
                worker_config_json = os.getenv(env_var_name)

                if (
                    worker_config_json
                    and env_var_name == _WORKER_CONFIG_ENV_VAR_NAME_OLD
                ):
                    warn(
                        f"{_WORKER_CONFIG_ENV_VAR_NAME_OLD!r} env var "
                        f"is deprecated. Use {_WORKER_CONFIG_ENV_VAR_NAME!r}",
                        DeprecationWarning,
                        stacklevel=2,
                    )

                if worker_config_json:
                    self._worker_config = WorkerConfig(
                        **orjson.loads(worker_config_json)
                    )
                    break

        self._app = FastAPI(
            lifespan=self._lifespan,
            docs_url="/",
            root_path=os.getenv("TFY_SERVICE_ROOT_PATH"),
        )
        self._app.get("/health")(self._healthy_route_handler)
        self._app.get("/ready")(self._ready_route_handler)
        self._app.post("/process")(self._process_route_handler)

        if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
            self._app.mount("/metrics", _make_multiprocess_metrics_app())
        else:
            self._app.mount("/metrics", make_asgi_app())

        self._worker = None

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        logger.info("Invoking the processor init method")
        self._processor.init()
        logger.info("Processor init method execution completed")
        worker_task = None
        if self._worker_config:
            logger.info("Starting processor runner")
            loop = asyncio.get_running_loop()
            self._worker = Worker(
                worker_config=self._worker_config,
                processor=self._processor,
            )
            worker_task = loop.create_task(self._worker.run())
            logger.info("Started processor runner")
        else:
            logger.warning(
                "Processor runner config not passed. Processor runner will not start.\n"
                "Only `/process` API will be available."
            )
        yield
        logger.info("Shutting down worker")

        try:
            if self._worker:
                self._worker.stop()
                await worker_task
        except Exception:
            logger.exception("Exception raised while stopping the worker.")

        if os.getenv("PROMETHEUS_MULTIPROC_DIR"):
            multiprocess.mark_process_dead(os.getpid())

    def _ready_route_handler(self):
        return ""

    def _healthy_route_handler(self):
        if self._worker and not self._worker.healthy:
            raise HTTPException(status_code=500, detail="Worker not healthy")
        return ""

    def _process_route_handler(self, body: InputMessage) -> Response:
        start = time.perf_counter()
        output = self._processor.process(body)
        time_taken_for_request = time.perf_counter() - start
        logger.info("Time taken to process request: %f seconds", time_taken_for_request)

        return Response(
            content=self._processor.output_serializer(
                OutputMessage(
                    request_id=body.request_id,
                    body=output,
                    status=ProcessStatus.SUCCESS,
                )
            ),
        )

    @property
    def app(self) -> FastAPI:
        return self._app
