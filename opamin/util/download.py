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
from io import BytesIO
from logging import getLogger
from pathlib import Path
from typing import BinaryIO, Union

import requests
from tqdm import tqdm

import opamin


# Adapted from: https://stackoverflow.com/a/37573701/211404
def download_file_with_progressbar(
    url: str, dest: Path, description: str = None
) -> None:
    logger = getLogger(opamin.__name__)
    logger.debug('Downloading url "{}" to file "{}"...'.format(url, dest))

    response = requests.get(url, stream=True)

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
        logger.warning(
            "  Downloaded file size mismatch, expected {} bytes "
            "got {} bytes.".format(total_size, wrote_bytes)
        )


def sha256sum(file: Union[Path, BinaryIO, BytesIO]) -> str:
    logger = getLogger(opamin.__name__)

    fd = file
    try:
        if isinstance(file, Path):
            logger.debug('Calculating checksum of file "{}".'.format(file))
            fd = file.open("rb")
        else:
            logger.debug("Calculating checksum of binary buffer.")

        # Taken from: https://stackoverflow.com/a/44873382/211404
        h = hashlib.sha256()
        for buffer in iter(lambda: fd.read(128 * 1024), b""):
            h.update(buffer)
    finally:
        if isinstance(file, Path):
            fd.close()

    return h.hexdigest()
