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
from logging import Logger, getLogger
from lzma import LZMAFile
from pathlib import Path
from types import TracebackType
from typing import IO, Optional, Type, cast

from overrides import overrides
from tqdm import tqdm
from typing_extensions import Final
from zstandard import ZstdDecompressor

_LOGGER: Final[Logger] = getLogger(__name__)


class DecompressingTextIOWrapper(TextIOWrapper):
    def __init__(
        self,
        path: Path,
        *,
        encoding: str,
        warn_uncompressed: bool = True,
        progress_bar: bool = False,
        progress_bar_desc: Optional[str] = None,
    ):
        self.path = path

        self._fp = path.open("rb")
        self._fin: IO[bytes]
        if path.suffix == ".gz":
            self._fin = cast(IO[bytes], GzipFile(fileobj=self._fp))
        elif path.suffix == ".bz2":
            self._fin = BZ2File(self._fp)
        elif path.suffix == ".xz":
            self._fin = LZMAFile(self._fp)
        elif path.suffix == ".zst":
            self._fin = cast(IO[bytes], ZstdDecompressor().stream_reader(self._fp))
        else:
            if warn_uncompressed:
                _LOGGER.warning(
                    f"Could not detect compression type of file '{path}' from its "
                    "extension, treating as uncompressed file."
                )
            self._fin = self._fp

        self._progress_bar: Optional[tqdm[None]] = None
        if progress_bar:
            self._progress_bar = tqdm(
                desc=progress_bar_desc or self.path.name,
                total=self.size(),
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            )

        super().__init__(self._fin, encoding=encoding)

    def size(self) -> int:
        return self.path.stat().st_size

    @overrides
    def read(self, n: Optional[int] = -1) -> str:
        result = super().read(n)
        if self._progress_bar is not None:
            self._progress_bar.update(self.tell() - self._progress_bar.n)
        return result

    @overrides
    def readline(self, size: int = -1) -> str:
        result = super().readline(size)
        if self._progress_bar is not None:
            self._progress_bar.update(self.tell() - self._progress_bar.n)
        return result

    @overrides
    def tell(self) -> int:
        """Tells the number of compressed bytes that have already been read."""
        return self._fp.tell()

    @overrides
    def __enter__(self) -> "DecompressingTextIOWrapper":
        return cast(DecompressingTextIOWrapper, super().__enter__())

    # In the following the type-comment is used to have Mypy ignore that this method
    # definition does not match the supertype (no idea why that can be or to fix it).
    # The noqa-comment is to have flake8 not print an error on not knowing the
    # ignore[override] type, which is a Mypy-annotation flake8 doesn't know about.
    @overrides
    def __exit__(  # type: ignore[override]  # noqa: F821
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self._fp.close()
        self._fin.close()
        if self._progress_bar is not None:
            self._progress_bar.close()
        return super().__exit__(exc_type, exc_value, traceback)
