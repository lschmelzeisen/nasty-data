#
# Copyright 2019-2020 Lukas Schmelzeisen
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import sys
from argparse import ArgumentParser
from argparse import Namespace as ArgumentNamespace
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Tuple

import toml

import opamin
import opamin.commands
from opamin.commands.command import Command
from opamin.commands.reddit import (  # noqa: F401
    CommandDownloadPushshift,
    CommandReddit,
    CommandRedditConfigureIndex,
    CommandRedditDeleteIndex,
    CommandRedditIndexFile,
)
from opamin.util.logging import setup_logging


def main(argv: List[str] = sys.argv[1:]):
    args, command = load_args(argv)

    setup_logging(args.log_level)
    logger = getLogger(opamin.__name__)
    logger.debug("Raw arguments: {}".format(argv))
    logger.debug("Parsed arguments: {}".format(vars(args)))
    logger.debug("Parsed command: {}.{}".format(command.__module__, command.__name__))

    config = load_config()

    command(args, config).run()


def load_args(argv: List[str]) -> Tuple[ArgumentNamespace, Command.__class__]:
    argparser = ArgumentParser(prog="opamin", description="TODO", add_help=False)

    argparser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    argparser.add_argument(
        "-v",
        "--version",
        action="version",
        version="%(prog)s development version",
        help="Show program's version number and exit.",
    )
    argparser.add_argument(
        "--log-level",
        metavar="<level>",
        type=str,
        choices=["DEBUG", "INFO", "WARN", "ERROR"],
        default="INFO",
        dest="log_level",
        help="Set logging level (DEBUG, INFO, WARN, ERROR).",
    )

    def add_subcommands(argparser, cls, depth=1):
        if len(cls.__subclasses__()):
            title = ("sub" * (depth - 1)) + "command"
            subparsers = argparser.add_subparsers(
                title=title, metavar="<{}>".format(title)
            )
            subparsers.required = True

            for subclass in cls.__subclasses__():
                subparser = subparsers.add_parser(
                    subclass.command,
                    aliases=subclass.aliases,
                    help=subclass.description,
                    description=subclass.description,
                    add_help=False,
                )
                subparser.set_defaults(command=subclass)
                subparser.add_argument(
                    "-h",
                    "--help",
                    action="help",
                    default=argparse.SUPPRESS,
                    help="Show this help message and exit.",
                )
                subclass.config_argparser(subparser)
                add_subcommands(subparser, subclass, depth=depth + 1)

    add_subcommands(argparser, Command)

    args = argparser.parse_args(argv)

    return args, args.command


def load_config(path: Path = Path(__file__).parent.parent / "config.toml") -> Dict:
    logger = getLogger(opamin.__name__)

    if not path.exists():
        logger.error(
            'Could not find config file in "{}". Make sure you copy '
            "the example config file to this location and set your "
            "personal settings/secrets.".format(path)
        )
        sys.exit()

    logger.debug('Loading config from "{}"...'.format(path))
    with path.open(encoding="UTF-8") as fin:
        config = toml.load(fin)

    def hide_secrets(value, hidden=False):
        if isinstance(value, dict):
            return {
                k: hide_secrets(v, hidden=(hidden or ("secret" in k)))
                for k, v in value.items()
            }
        return "<hidden>" if hidden else value

    logger.debug("Loaded config:")
    for line in toml.dumps(hide_secrets(config)).splitlines():
        logger.debug("  " + line)

    return config


if __name__ == "__main__":
    main()
