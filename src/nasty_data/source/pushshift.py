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
import json
import re
from datetime import date
from enum import Enum
from itertools import chain
from json import JSONDecodeError
from logging import Logger, getLogger
from pathlib import Path
from typing import Counter, Iterator, Mapping, Optional, Tuple

import requests
from elasticsearch_dsl import Date, InnerDoc, Keyword, Object
from typing_extensions import Final

from nasty_data.document.reddit import RedditDocument
from nasty_utils import (
    DecompressingTextIOWrapper,
    FileNotOnServerError,
    advance_date_by_months,
    download_file_with_progressbar,
    format_yyyy_mm,
    format_yyyy_mm_dd,
    parse_yyyy_mm,
    sha256sum,
)

_LOGGER: Final[Logger] = getLogger(__name__)


class PushshiftDumpType(Enum):
    LINKS = enum.auto()
    COMMENTS = enum.auto()


_PUSHSHIFT_URL = {
    PushshiftDumpType.LINKS: "https://files.pushshift.io/reddit/submissions/",
    PushshiftDumpType.COMMENTS: "https://files.pushshift.io/reddit/comments/",
}
_PUSHSHIFT_SHA256SUMS_URL = {
    PushshiftDumpType.LINKS: _PUSHSHIFT_URL[PushshiftDumpType.LINKS] + "sha256sums.txt",
    PushshiftDumpType.COMMENTS: _PUSHSHIFT_URL[PushshiftDumpType.COMMENTS]
    + "sha256sum.txt",
}
_PUSHSHIFT_FILE_PATTERNS = {
    # File names ordered after preference.
    PushshiftDumpType.LINKS: [
        r"^RS_(\d{4}-\d{2}).zst$",
        r"^RS_(\d{4}-\d{2}).xz$",
        r"^RS_(\d{4}-\d{2}).bz2$",
        r"^RS_v2_(\d{4}-\d{2}).xz$",
    ],
    PushshiftDumpType.COMMENTS: [
        r"^RC_(\d{4}-\d{2}).zst$",
        r"^RC_(\d{4}-\d{2}).xz$",
        r"^RC_(\d{4}-\d{2}).bz2$",
    ],
}
_PUSHSHIFT_EARLIEST_SINCE = {
    PushshiftDumpType.LINKS: date(year=2005, month=6, day=1),
    PushshiftDumpType.COMMENTS: date(year=2005, month=12, day=1),
}


def download_pushshift_dumps(
    directory: Path,
    *,
    dump_type: Optional[PushshiftDumpType] = None,
    since: Optional[date] = None,
    until: Optional[date] = None,
) -> None:
    log_dump_type = (
        " and ".join([t.name.lower() for t in PushshiftDumpType])
        if not dump_type
        else dump_type.name.lower()
    )
    log_since = format_yyyy_mm(since) if since is not None else "earliest"
    log_until = format_yyyy_mm(until) if until is not None else "latest"
    _LOGGER.info(
        f"Downloading Pushshift dumps of Reddit {log_dump_type} from {log_since} to "
        f"{log_until} to '{directory}'."
    )

    Path.mkdir(directory, parents=True, exist_ok=True)
    for type_ in PushshiftDumpType:
        if dump_type is not None and type_ != dump_type:
            continue

        checksums = _download_pushshift_checksums(type_)

        current_date = since or _PUSHSHIFT_EARLIEST_SINCE[type_]
        while True:
            try:
                _download_pushshift_dump(directory, type_, current_date, checksums)
            except FileNotOnServerError:
                # No dump available for selected date range.
                if since == current_date or until:
                    raise
                break

            if current_date == until:
                break
            current_date = advance_date_by_months(current_date, num_months=1)


def _download_pushshift_checksums(dump_type: PushshiftDumpType) -> Mapping[str, str]:
    checksums_raw = requests.get(_PUSHSHIFT_SHA256SUMS_URL[dump_type]).content.decode(
        "ascii"
    )
    checksums = {}
    for line in checksums_raw.splitlines():
        if not line.strip():
            continue
        checksum, file = line.split()
        checksums[file] = checksum
    return checksums


def _download_pushshift_dump(
    directory: Path,
    dump_type: PushshiftDumpType,
    date_: date,
    checksums: Mapping[str, str],
) -> None:
    def file_name_from_pattern(pattern: str) -> Path:
        return directory / pattern.lstrip("^").rstrip("$").replace(
            r"(\d{4}-\d{2})", format_yyyy_mm(date_)
        )

    # Doing the same file name resolution in separate loop before actual download so
    # that no HTTP requests are fired, if a file already exists.
    for file_pattern in _PUSHSHIFT_FILE_PATTERNS[dump_type]:
        target = file_name_from_pattern(file_pattern)
        if target.exists():
            _LOGGER.debug(f"File {target.name} already exists, skipping.")
            return

    for file_pattern in _PUSHSHIFT_FILE_PATTERNS[dump_type]:
        target = file_name_from_pattern(file_pattern)
        target_tmp = target.with_name(target.name + ".tmp")
        try:
            download_file_with_progressbar(
                _PUSHSHIFT_URL[dump_type] + target.name, target_tmp, target.name
            )
        except FileNotOnServerError:
            continue

        break

    else:  # for-loop did not exit via break.
        raise FileNotOnServerError(
            f"No Reddit {dump_type.name.lower()} dump for {format_yyyy_mm(date_)} "
            "available."
        )

    expected_checksum = checksums.get(target.name)
    if expected_checksum is None:
        _LOGGER.info(
            f"Download for file {target.name} complete, but no checksum available."
        )
    else:
        checksum = sha256sum(target_tmp)
        if checksum != expected_checksum:
            target_tmp.unlink()
            raise ValueError(
                f"Download for file {target.name} complete, but calculated checksum "
                f"'{checksum}' does not match expected '{expected_checksum}'. Deleted "
                "file. Restart to try again."
            )
    target_tmp.rename(target)


def sample_pushshift_dumps(directory: Path) -> None:
    _LOGGER.info(f"Sampling Pushshift dumps in '{directory}'.")

    samples = []
    for file in sorted(directory.iterdir()):
        for file_pattern in chain.from_iterable(_PUSHSHIFT_FILE_PATTERNS.values()):
            if re.match(file_pattern, file.name):
                samples.append(_sample_pushshift_dump(file))
                break

    _LOGGER.info("Concatenating individual samples.")
    with (directory / "all.sample").open("w", encoding="UTF-8") as fout:
        for sample in samples:
            with sample.open("r", encoding="UTF-8") as fin:
                for line in fin:
                    fout.write(line)


def _sample_pushshift_dump(dump_file: Path) -> Path:
    sample_file = dump_file.parent / (dump_file.name + ".sample")
    if sample_file.exists():
        _LOGGER.debug(f"Sample of {dump_file.name} already exists, skipping.")
        return sample_file

    keys = Counter[str]()

    sample_file_tmp = sample_file.with_name(sample_file.name + ".tmp")
    with sample_file_tmp.open("w", encoding="UTF-8") as fout:
        for document_dict in load_document_dicts_from_pushshift_dump(dump_file):
            keys.update(document_dict.keys())
            if all(keys[key] > 100 for key in document_dict.keys()):
                continue

            fout.write(json.dumps(document_dict) + "\n")

    sample_file_tmp.rename(sample_file)
    return sample_file


class PushshiftDumpMeta(InnerDoc):
    dump_file = Keyword()
    dump_type = Keyword()
    dump_date = Date()


class PushshiftRedditDocument(RedditDocument):
    pushshift_dump_meta = Object(PushshiftDumpMeta)

    @classmethod
    def meta_field(cls) -> Tuple[str, str]:
        return "pushshift_dump_meta", "dump_file"


def load_document_dicts_from_pushshift_dump(
    dump_file: Path, *, progress_bar: bool = True,
) -> Iterator[Mapping[str, object]]:
    pushshift_dump_meta: Optional[Mapping[str, object]] = None
    for dump_type, file_pattern in (
        (t, p) for t, ps in _PUSHSHIFT_FILE_PATTERNS.items() for p in ps
    ):
        m = re.match(file_pattern, dump_file.name)
        if m:
            pushshift_dump_meta = {
                "dump_file": dump_file.name,
                "dump_type": dump_type.name,
                "dump_date": format_yyyy_mm_dd(parse_yyyy_mm(m.group(1))),
            }
            break

    with DecompressingTextIOWrapper(
        dump_file, encoding="UTF-8", progress_bar=progress_bar, warn_uncompressed=False
    ) as fin:
        for line_no, line in enumerate(fin):
            # For some reason, there is at least one line (specifically, line 29876 in
            # file RS_2011-01.bz2) that contains NUL characters at the beginning of it,
            # which we remove with the following.
            line = line.lstrip("\0")

            try:
                document_dict = json.loads(line)
            except JSONDecodeError:
                _LOGGER.error(f"Error in line {line_no} of file '{dump_file}'.")
                raise

            document_dict["pushshift_dump_meta"] = pushshift_dump_meta
            yield document_dict
