import logging
import os

log = logging.getLogger("mindsdb.main")
if log.level == logging.NOTSET:
    log_level = os.getenv('LOGLEVEL', 'INFO')
    log.setLevel(getattr(logging, log_level))
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    log.addHandler(console_handler)

from .cache import Cache
