import logging
import os
import sys

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
log_level = logging.getLevelName(LOG_LEVEL.upper())


def _setup_logging(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(module)s:%(funcName)s:%(lineno)d - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = _setup_logging("async_processor")
_setup_logging("kafka")
