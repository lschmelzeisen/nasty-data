import logging
from logging import getLogger

import elasticsearch
from tqdm import tqdm

import opamin


# from https://github.com/tqdm/tqdm/issues/313#issuecomment-346819396
class TqdmStream:
    @classmethod
    def write(cls, msg):
        tqdm.write(msg, end='')


def setup_logging():
    logging.basicConfig(
        format='{asctime} {levelname}({name}): {message}',
        style='{',
        level=logging.INFO,
        stream=TqdmStream)

    getLogger(opamin.__name__).setLevel(logging.DEBUG)
    getLogger(elasticsearch.__name__).setLevel(logging.WARN)
