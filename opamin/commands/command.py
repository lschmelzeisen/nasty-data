from argparse import ArgumentParser, Namespace
from logging import getLogger
from typing import Dict, List

import opamin


class Command:
    command: str = 'overwrite-me'
    aliases: List[str] = []
    description: str = 'Overwrite me!'

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        raise NotImplementedError()

    def __init__(self, args: Namespace, config: Dict):
        self.args = args
        self.config = config
        self.logger = getLogger(opamin.__name__)

    def run(self) -> None:
        raise NotImplementedError()
