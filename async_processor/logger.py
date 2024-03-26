import logging
import os

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
log_level = logging.getLevelName(LOG_LEVEL.upper())

logger = logging.getLogger("async_processor")
logger.setLevel(log_level)
handler = logging.StreamHandler()
handler.setLevel(log_level)
formatter = logging.Formatter(
    "%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(module)s:%(funcName)s:%(lineno)d - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
