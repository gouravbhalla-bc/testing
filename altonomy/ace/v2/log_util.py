import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from altonomy.ace import config


def get_v2_logger(log_name: str):
    log_dir = os.path.join("logs", "ace", "v2")
    os.makedirs(log_dir, exist_ok=True)
    logging_format = logging.Formatter('%(levelname)s:%(filename)s:%(lineno)d:%(asctime)s:%(message)s')
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)

    if config.LOG_TO_FILE.upper() == "TRUE":
        log_file_name = os.path.join(log_dir, log_name)
        handler = RotatingFileHandler(f"{log_file_name}.log", mode='a', maxBytes=5 * 1024 * 1024, backupCount=5, encoding=None, delay=0)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging_format)
        logger.addHandler(handler)

    if config.LOG_TO_STDOUT.upper() == "TRUE":
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging_format)
        logger.addHandler(handler)

    return logger
