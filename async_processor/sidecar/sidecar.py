import time

import requests
from pydantic import BaseSettings, confloat

from async_processor import InputMessage, Processor
from async_processor.logger import logger


class Settings(BaseSettings):
    destination_url: str
    request_timeout: confloat(gt=0) = 3.0

    class Config:
        env_prefix = "TFY_ASYNC_PROCESSOR_SIDECAR_"


settings = Settings()


class SidecarProcessor(Processor):
    def init(self):
        while True:
            try:
                requests.head(settings.destination_url)
                break
            except Exception as ex:
                logger.warning(
                    "Cannot connect to %s retrying in 1 second. " "%s",
                    settings.destination_url,
                    str(ex),
                )
                time.sleep(1.0)

    def process(self, input_message: InputMessage) -> str:
        response = requests.post(
            settings.destination_url,
            json=input_message.body,
            timeout=settings.request_timeout,
        )
        response.raise_for_status()
        return response.text


app = SidecarProcessor().build_app()
