import logging
from logging import getLogger

import elasticsearch
import urllib3
from tqdm import tqdm


# from https://github.com/tqdm/tqdm/issues/313#issuecomment-346819396
class TqdmStream:
    @classmethod
    def write(cls, msg):
        tqdm.write(msg, end='')


def setup_logging(level: str):
    numeric_level = getattr(logging, level)

    logging.basicConfig(
        format='{asctime} {levelname}({name}): {message}',
        style='{',
        level=numeric_level,
        stream=TqdmStream)

    getLogger(elasticsearch.__name__).setLevel(logging.INFO)
    getLogger(urllib3.__name__).setLevel(logging.INFO)
