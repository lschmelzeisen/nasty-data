from bz2 import BZ2File
from gzip import GzipFile
from io import TextIOWrapper
from lzma import LZMAFile
from pathlib import Path

from zstandard import ZstdDecompressor


class DecompressingTextIOWrapper(TextIOWrapper):
    def __init__(self, path: Path, encoding: str,
                 warn_uncompressed: bool = True):
        self.path = path

        self._fp = path.open('rb')
        if path.suffix == '.gz':
            self._fin = GzipFile(self._fp)
        elif path.suffix == '.bz2':
            self._fin = BZ2File(self._fp)
        elif path.suffix == '.xz':
            self._fin = LZMAFile(self._fp)
        elif path.suffix == '.zst':
            self._fin = ZstdDecompressor().stream_reader(self._fp)
        else:
            if warn_uncompressed:
                print('WARNING: Could not detect compression type of file "{}" '
                      'from its extension, treating as uncompressed file.'
                      .format(path))
            self._fin = self._fp
        super().__init__(self._fin, encoding=encoding)

    def size(self):
        return self.path.stat().st_size

    def tell(self):
        """Tells the number of compressed bytes that have already been read."""
        return self._fp.tell()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._fp.close()
        self._fin.close()
