import logging
import os
import sys

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

if LOG_LEVEL == "DEBUG":
    logger = logging.getLogger("kafka")
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.setLevel(logging.DEBUG)
