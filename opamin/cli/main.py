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
import logging
import sys
from argparse import ArgumentParser
from collections import defaultdict
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Type, cast

import toml
from typing_extensions import Final

import opamin

from .._util.argparse_ import SingleMetavarHelpFormatter
from .._util.logging_ import setup_logging
from ._command import _Command
from ._reddit._configure_index_reddit_command import _ConfigureIndexRedditCommand
from ._reddit._delete_index_reddit_command import _DeleteIndexRedditCommand
from ._reddit._download_pushshift_reddit_command import _DownloadPushshiftRedditCommand
from ._reddit._index_file_reddit_command import _IndexDumpRedditCommand
from ._reddit._reddit_command import _RedditCommand
from ._reddit._sample_pushshift_reddit_command import _SamplePushshiftRedditCommand

LOGGER: Final[Logger] = getLogger(__name__)


def main(argv: Optional[Sequence[str]] = None) -> None:
    if argv is None:  # pragma: no cover
        argv = sys.argv[1:]

    command = _load_args(argv)
    command.run()


def _load_args(argv: Sequence[str]) -> _Command:
    subcommands_by_command_type: Mapping[
        Type[_Command], Sequence[Type[_Command]]
    ] = defaultdict(
        list,
        {
            _Command: [_RedditCommand],
            _RedditCommand: [
                _DownloadPushshiftRedditCommand,
                _SamplePushshiftRedditCommand,
                _ConfigureIndexRedditCommand,
                _IndexDumpRedditCommand,
                _DeleteIndexRedditCommand,
            ],
        },
    )
    subparser_by_command_type = {}

    def add_subcommands(
        argparser: ArgumentParser, command: Type[_Command], *, prog: str, depth: int = 1
    ) -> None:
        subcommands = subcommands_by_command_type[command]
        if not subcommands:
            return

        title = ("sub" * (depth - 1)) + "command"
        subparsers = argparser.add_subparsers(
            title=title[0].upper() + title[1:] + "s",
            description=(
                "The following commands (and abbreviations) are available, each "
                "supporting the help option."
            ),
            metavar="<" + title.upper() + ">",
            prog=prog,
        )
        subparsers.required = True

        for subcommand in subcommands:
            subparser = subparsers.add_parser(
                subcommand.command(),
                aliases=subcommand.aliases(),
                help=subcommand.description(),
                description=subcommand.description(),
                add_help=False,
                formatter_class=SingleMetavarHelpFormatter,
            )
            subparser.set_defaults(command=subcommand)
            subparser_by_command_type[subcommand] = subparser

            add_subcommands(
                subparser,
                subcommand,
                prog=prog + " " + subcommand.command(),
                depth=depth + 1,
            )

            subcommand.config_argparser(subparser)
            _config_general_args(subparser)

    argparser = ArgumentParser(
        prog="opamin",
        description="Open Argument Mining Toolkit",
        add_help=False,
        formatter_class=SingleMetavarHelpFormatter,
    )

    # Ignoring typing on the following line because there does not seem to be a way how
    # to type this correctly yet, since _Command is an abstract class.
    # See https://github.com/python/mypy/issues/5374
    add_subcommands(argparser, _Command, prog="opamin")  # type: ignore
    _config_general_args(argparser)

    args = argparser.parse_args(argv)

    numeric_log_level = getattr(logging, args.log_level)
    setup_logging(numeric_log_level)
    LOGGER.debug("Opamin version: {}".format(opamin.__version__))
    LOGGER.debug("Raw arguments: {}".format(argv))
    LOGGER.debug("Parsed arguments: {}".format(vars(args)))
    LOGGER.debug(
        "Parsed command {}.{}".format(args.command.__module__, args.command.__name__)
    )

    config = _load_config(args.config, argparser)

    command: _Command = args.command(args, config)
    command.validate_arguments(subparser_by_command_type[args.command])

    return command


def _config_general_args(argparser: ArgumentParser) -> None:
    g = argparser.add_argument_group("General Arguments")

    # The following line & the add_help=False above is to be able to customize
    # the help message. See: https://stackoverflow.com/a/35848313/211404
    g.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )

    g.add_argument(
        "-v",
        "--version",
        action="version",
        version="%(prog)s " + opamin.__version__,
        help="Show program's version number and exit.",
    )

    g.add_argument(
        "--config",
        metavar="<CONFIG>",
        type=Path,
        default=Path(opamin.__file__).parent.parent / "config.toml",
        help="Config file path (default: config.toml in source dir).",
    )

    g.add_argument(
        "--log-level",
        metavar="<LEVEL>",
        type=str,
        choices=["DEBUG", "INFO", "WARN", "ERROR"],
        default="INFO",
        help="Logging level (DEBUG, INFO, WARN, ERROR.)",
    )


def _load_config(path: Path, argparser: ArgumentParser) -> Mapping[str, object]:
    if not path.exists():
        argparser.error(
            "Could not find config file in '{}'. Make sure you copy the example config "
            "file to this location and set your personal settings/secrets.".format(path)
        )

    LOGGER.debug("Loading config from '{}'...".format(path))
    with path.open(encoding="UTF-8") as fin:
        config = toml.load(fin)

    def hide_secrets(value: object, hidden: bool = False) -> object:
        if isinstance(value, Mapping):
            return {
                k: hide_secrets(v, hidden=(hidden or "secret" in k))
                for k, v in value.items()
            }
        return "<hidden>" if hidden else value

    LOGGER.debug("Loaded config:")
    for line in toml.dumps(cast(Mapping[str, Any], hide_secrets(config))).splitlines():
        LOGGER.debug("  " + line)

    return config