import logging

logger = logging.getLogger("async_processor")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s:%(name)s:%(levelname)s:%(process)d:%(module)s:%(funcName)s:%(lineno)d - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
