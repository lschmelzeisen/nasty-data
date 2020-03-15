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

import hashlib
from http import HTTPStatus
from io import BytesIO
from logging import Logger, getLogger
from pathlib import Path
from typing import BinaryIO, Union

import requests
from tqdm import tqdm
from typing_extensions import Final

from ..errors import FileNotOnServerError

LOGGER: Final[Logger] = getLogger(__name__)


# Adapted from: https://stackoverflow.com/a/37573701/211404
def download_file_with_progressbar(url: str, dest: Path, description: str) -> None:
    response = requests.get(url, stream=True)
    if response.status_code != HTTPStatus.OK.value:
        status = HTTPStatus(response.status_code)
        raise FileNotOnServerError(
            f"Unexpected status code {status.value} {status.name}."
        )

    LOGGER.debug(f"Downloading url '{url}' to file '{dest}'...")

    total_size = int(response.headers.get("content-length", 0))
    chunk_size = 2 ** 12  # 4 Kib

    wrote_bytes = 0
    with dest.open("wb") as fout, tqdm(
        desc=description, total=total_size, unit="B", unit_scale=True, unit_divisor=1024
    ) as progress_bar:
        for chunk in response.iter_content(chunk_size):
            wrote_bytes += fout.write(chunk)
            progress_bar.update(len(chunk))

    if total_size != 0 and total_size != wrote_bytes:
        LOGGER.warning(
            f"  Downloaded file size mismatch, expected {total_size} bytes got "
            f"{wrote_bytes} bytes."
        )


def sha256sum(file: Union[Path, BinaryIO, BytesIO]) -> str:
    if isinstance(file, Path):
        fd = file.open("rb")
    else:
        fd = file

    try:
        # Taken from: https://stackoverflow.com/a/44873382/211404
        h = hashlib.sha256()
        for buffer in iter(lambda: fd.read(128 * 1024), b""):
            h.update(buffer)
    finally:
        if isinstance(file, Path):
            fd.close()

    return h.hexdigest()
