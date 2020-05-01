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

from argparse import ArgumentParser
from pathlib import Path
from typing import Sequence

from overrides import overrides

from ..._util.elasticsearch import establish_elasticsearch_connection
from ...data.reddit import ensure_reddit_index_available
from .._command import Command


class IndexDumpRedditCommand(Command):
    @classmethod
    @overrides
    def command(cls) -> str:
        return "index-dump"

    @classmethod
    @overrides
    def aliases(cls) -> Sequence[str]:
        return ["id", "index"]

    @classmethod
    @overrides
    def description(cls) -> str:
        return "Add contents of a given Reddit dump to Elasticsearch."

    @classmethod
    @overrides
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        argparser.add_argument(
            "file",
            metavar="<FILE>",
            type=Path,
            help="Add all Reddit posts in a given file to Elasticsearch index.",
        )

    @overrides
    def run(self) -> None:
        establish_elasticsearch_connection(self._config)
        ensure_reddit_index_available()
