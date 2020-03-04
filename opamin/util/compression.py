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

from bz2 import BZ2File
from gzip import GzipFile
from io import TextIOWrapper
from logging import getLogger
from lzma import LZMAFile
from pathlib import Path

from zstandard import ZstdDecompressor

import opamin


class DecompressingTextIOWrapper(TextIOWrapper):
    def __init__(self, path: Path, encoding: str, warn_uncompressed: bool = True):
        logger = getLogger(opamin.__name__)

        self.path = path

        self._fp = path.open("rb")
        if path.suffix == ".gz":
            self._fin = GzipFile(self._fp)
        elif path.suffix == ".bz2":
            self._fin = BZ2File(self._fp)
        elif path.suffix == ".xz":
            self._fin = LZMAFile(self._fp)
        elif path.suffix == ".zst":
            self._fin = ZstdDecompressor().stream_reader(self._fp)
        else:
            if warn_uncompressed:
                logger.warning(
                    'Could not detect compression type of file "{}" '
                    "from its extension, treating as uncompressed "
                    "file.".format(path)
                )
            self._fin = self._fp
        super().__init__(self._fin, encoding=encoding)

    def size(self) -> int:
        return self.path.stat().st_size

    def tell(self) -> int:
        """Tells the number of compressed bytes that have already been read."""
        return self._fp.tell()

    def __enter__(self) -> "DecompressingTextIOWrapper":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._fp.close()
        self._fin.close()
