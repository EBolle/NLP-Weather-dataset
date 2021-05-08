"""Configuration of the stdout logger output."""

import logging

logger = logging.getLogger(__name__)

handler = logging.StreamHandler()
stream_format = "%(levelname)s %(asctime)s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
set_stream_format = logging.Formatter(stream_format)
handler.setFormatter(set_stream_format)
logger.addHandler(handler)

logger.setLevel(logging.DEBUG)