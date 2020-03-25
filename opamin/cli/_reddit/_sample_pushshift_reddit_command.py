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

import json
from argparse import ArgumentParser
from logging import Logger, getLogger
from pathlib import Path
from typing import Counter, Mapping, Sequence, cast

from overrides import overrides
from typing_extensions import Final

from ..._util.compression import DecompressingTextIOWrapper
from .._command import _Command

LOGGER: Final[Logger] = getLogger(__name__)


def _sample_dumps(pushshift_dir: Path) -> None:
    LOGGER.info("Sampling all downloaded Pushshift dumps.")

    samples = []
    for dump in pushshift_dir.iterdir():
        if dump.suffix not in [".zst", ".bz2", ".xz"]:
            continue
        samples.append(_sample_dump(dump))

    LOGGER.info("Aggregating individual samples.")
    with (pushshift_dir / "all.sample").open("w", encoding="UTF-8") as fout:
        for sample in samples:
            with sample.open("r", encoding="UTF-8") as fin:
                for line in fin:
                    fout.write(line)


def _sample_dump(dump: Path) -> Path:
    sample = dump.parent / (dump.name + ".sample")
    if sample.exists():
        LOGGER.debug(f"Sample of {dump.name} already exists, skipping.")
        return sample

    keys = Counter[str]()

    sample_tmp = dump.parent / (dump.name + ".sample.tmp")
    with DecompressingTextIOWrapper(
        dump, encoding="UTF-8", progress_bar=True
    ) as fin, sample_tmp.open("w", encoding="UTF-8") as fout:
        for i, line in enumerate(fin):
            # For some reason, there is at least one line (specifically,
            # line 29876 in file RS_2011-01.bz2) that contains NUL
            # characters at the beginning of it, which we remove with
            # the following.
            line = line.lstrip("\0")

            post = json.loads(line.strip())

            keys.update(post.keys())
            if all(keys[key] > 100 for key in post.keys()):
                continue

            fout.write(json.dumps(post) + "\n")

    sample_tmp.rename(sample)
    return sample


class _SamplePushshiftRedditCommand(_Command):
    @classmethod
    @overrides
    def command(cls) -> str:
        return "sample-pushshift"

    @classmethod
    @overrides
    def aliases(cls) -> Sequence[str]:
        return ["s"]

    @classmethod
    @overrides
    def description(cls) -> str:
        return "Produce a sample of all downloaded Pushshift dumps."

    @classmethod
    @overrides
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    @overrides
    def run(self) -> None:
        pushshift_dir = Path(
            cast(Mapping[str, Mapping[str, str]], self._config)["data"]["pushshift-dir"]
        )
        _sample_dumps(pushshift_dir)
