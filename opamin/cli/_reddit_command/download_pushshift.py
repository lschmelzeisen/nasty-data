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

import enum
from argparse import ArgumentParser, ArgumentTypeError
from datetime import date, datetime, timedelta
from enum import Enum
from logging import Logger, getLogger
from pathlib import Path
from typing import Mapping, Optional, Sequence, cast

import requests
from overrides import overrides
from typing_extensions import Final

from ..._util.download import download_file_with_progressbar, sha256sum
from ...errors import ChecksumsNotMatchingError, FileNotOnServerError
from .._command import Command

LOGGER: Final[Logger] = getLogger(__name__)


class DumpType(Enum):
    LINKS = enum.auto()
    COMMENTS = enum.auto()


PUSHSHIFT_URL = {
    DumpType.LINKS: "https://files.pushshift.io/reddit/submissions/",
    DumpType.COMMENTS: "https://files.pushshift.io/reddit/comments/",
}
PUSHSHIFT_SHA256SUMS_URL = {
    DumpType.LINKS: PUSHSHIFT_URL[DumpType.LINKS] + "sha256sums.txt",
    DumpType.COMMENTS: PUSHSHIFT_URL[DumpType.COMMENTS] + "sha256sum.txt",
}
PUSHSHIFT_FILE_NAMES = {
    # File names ordered after preference.
    DumpType.LINKS: ["RS_{}.zst", "RS_{}.xz", "RS_{}.bz2", "RS_v2_{}.xz"],
    DumpType.COMMENTS: ["RC_{}.zst", "RC_{}.xz", "RC_{}.bz2"],
}
EARLIEST_SINCE = {
    DumpType.LINKS: date(year=2005, month=6, day=1),
    DumpType.COMMENTS: date(year=2005, month=12, day=1),
}


def _yyyy_mm_date(string: str) -> date:
    try:
        return datetime.strptime(string, "%Y-%m").date()
    except ValueError:
        raise ArgumentTypeError(
            f"Could not parse date: '{string}'. Make sure it is in YYYY-MM format."
        )


def _advance_date_by_one_month(current_date: date) -> date:
    tmp = current_date + timedelta(days=32)  # Enough days to surely reach next month.
    return tmp.replace(day=1)


def _download_dumps(
    type_: DumpType, since: Optional[date], until: Optional[date], pushshift_dir: Path,
) -> None:
    LOGGER.info(
        f"Download Pushshift dumps of Reddit {type_.name.lower()} "
        f"from '{since or 'earliest'} to '{until or 'latest'}'."
    )

    checksums = _download_checkums(type_)
    Path.mkdir(pushshift_dir, parents=True, exist_ok=True)

    current_date = since or EARLIEST_SINCE[type_]
    while True:
        try:
            _download_dump(type_, current_date, checksums, pushshift_dir)
        except FileNotOnServerError:
            # No dump available for selected date range.
            if since == current_date or until:
                raise
            break

        if current_date == until:
            break
        current_date = _advance_date_by_one_month(current_date)


def _download_dump(
    type_: DumpType,
    current_date: date,
    checksums: Mapping[str, str],
    pushshift_dir: Path,
) -> None:
    current_date_str = current_date.strftime("%Y-%m")

    # Doing the same work we do again later in a different loop so that no HTTP requests
    # are fired, if the file already exists.
    for file_name in PUSHSHIFT_FILE_NAMES[type_]:
        file_name = file_name.format(current_date_str)
        target = pushshift_dir / file_name
        if target.exists():
            LOGGER.debug(f"File {file_name} already exists, skipping.")
            return

    for file_name in PUSHSHIFT_FILE_NAMES[type_]:
        file_name = file_name.format(current_date_str)
        target = pushshift_dir / file_name
        target_tmp = pushshift_dir / (file_name + ".tmp")
        try:
            download_file_with_progressbar(
                PUSHSHIFT_URL[type_] + file_name, target_tmp, file_name
            )
        except FileNotOnServerError:
            continue

        break

    else:  # for-loop did not exit via break.
        raise FileNotOnServerError(
            f"No Reddit {type_.name.lower()} dump from {current_date_str} available."
        )

    expected_checksum = checksums.get(file_name)
    if expected_checksum is None:
        LOGGER.info(f"No checksum available for file {file_name}.")
    else:
        checksum = sha256sum(target_tmp)
        if checksum != expected_checksum:
            target_tmp.unlink()
            raise ChecksumsNotMatchingError(
                f"Calculated checksum '{checksum}' does not match expected "
                f"'{expected_checksum}'. Deleted file. Restart to try again."
            )
    target_tmp.rename(target)


def _download_checkums(type_: DumpType) -> Mapping[str, str]:
    checksums_raw = requests.get(PUSHSHIFT_SHA256SUMS_URL[type_]).content.decode(
        "ascii"
    )
    checksums = {}
    for line in checksums_raw.splitlines():
        if not line.strip():
            continue
        checksum, file = line.split()
        checksums[file] = checksum
    return checksums


class DownloadPushshiftRedditCommand(Command):
    @classmethod
    @overrides
    def command(cls) -> str:
        return "download-pushshift"

    @classmethod
    @overrides
    def aliases(cls) -> Sequence[str]:
        return ["dl"]

    @classmethod
    @overrides
    def description(cls) -> str:
        return "Download Pushshift Reddit dumps."

    @classmethod
    @overrides
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        g = argparser.add_argument_group(
            "Download Arguments", "Control which kind of dumps to download."
        )
        g.add_argument(
            "-a", "--all", action="store_true", help="Download all available dumps.",
        )
        g.add_argument(
            "-t",
            "--type",
            metavar="<TYPE>",
            type=str,
            choices=[type_.name for type_ in DumpType],
            default=None,
            help=f"Type of dump ({' or '.join(type_.name for type_ in DumpType)}).",
        )
        g.add_argument(
            "-s",
            "--since",
            metavar="<DATE>",
            type=_yyyy_mm_date,
            default=None,
            help=(
                "Month of earliest dump to download in YYYY-MM format (inclusive, "
                "defaults to earliest available)."
            ),
        )
        g.add_argument(
            "-u",
            "--until",
            metavar="<DATE>",
            type=_yyyy_mm_date,
            default=None,
            help=(
                "Month of latest dump to download in YYYY-MM format (inclusive, "
                "defaults to latest available)."
            ),
        )

    @overrides
    def validate_arguments(self, argparser: ArgumentParser) -> None:
        if self._args.all:
            if (
                self._args.type is not None
                or self._args.since is not None
                or self._args.until is not None
            ):
                argparser.error(
                    "-a (--all) must not be used together with any other arguments."
                )
        elif self._args.type is None:
            argparser.error("-t (--type) must be set.")
        elif self._args.until is not None:
            if self._args.since is not None:
                if self._args.until < self._args.since:
                    argparser.error(
                        "-u (--until) date must not be before -s (--since) date."
                    )
            elif (
                self._args.type == DumpType.LINKS.name
                and self._args.until < EARLIEST_SINCE[DumpType.LINKS]
            ):
                earliest = EARLIEST_SINCE[DumpType.LINKS].strftime("%Y-%m")
                argparser.error(
                    f"-u (--until) date must not be before {earliest} for LINKS."
                )
            elif (
                self._args.type == DumpType.COMMENTS.name
                and self._args.until < EARLIEST_SINCE[DumpType.COMMENTS]
            ):
                earliest = EARLIEST_SINCE[DumpType.COMMENTS].strftime("%Y-%m")
                argparser.error(
                    f"-u (--until) date must not be before {earliest} for COMMENTS."
                )

    @overrides
    def run(self) -> None:
        pushshift_dir = Path(
            cast(Mapping[str, Mapping[str, str]], self._config)["data"]["pushshift-dir"]
        )

        download_dumps_args = [self._args.since, self._args.until, pushshift_dir]
        if self._args.all:
            _download_dumps(DumpType.LINKS, *download_dumps_args)
            _download_dumps(DumpType.COMMENTS, *download_dumps_args)
        else:
            type_ = DumpType[self._args.type]
            _download_dumps(type_, *download_dumps_args)
