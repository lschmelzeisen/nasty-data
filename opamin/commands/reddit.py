from argparse import ArgumentParser
from typing import List

from opamin.commands import Command


class CommandReddit(Command):
    command: str = 'reddit'
    aliases: List[str] = ['r']
    description: str = 'TODO'

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    def run(self) -> None:
        pass


class CommandRedditDeleteIndex(CommandReddit):
    command: str = 'delete-index'
    aliases: List[str] = ['di', 'del']
    description: str = 'TODO'

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    def run(self) -> None:
        pass


class CommandRedditConfigureIndex(CommandReddit):
    command: str = 'configure-index'
    aliases: List[str] = ['ci', 'conf']
    description: str = 'TODO'

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    def run(self) -> None:
        pass


class CommandRedditIndexFile(CommandReddit):
    command: str = 'index-file'
    aliases: List[str] = ['if', 'index']
    description: str = 'TODO'

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    def run(self) -> None:
        pass
