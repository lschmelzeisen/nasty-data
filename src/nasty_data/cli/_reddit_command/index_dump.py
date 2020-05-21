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
from logging import Logger, getLogger
from pathlib import Path
from typing import Iterator, Mapping, Sequence

from elasticsearch.helpers import bulk
from overrides import overrides
from typing_extensions import Final

from nasty_data._util.elasticsearch import (
    debug_dynamic_mapping_difference,
    ensure_elasticsearch_index_available,
    establish_elasticsearch_connection,
)
from nasty_data.cli._command import Command
from nasty_data.data.reddit import REDDIT_INDEX, RedditPost, load_reddit_dicts_from_dump

_LOGGER: Final[Logger] = getLogger(__name__)


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
        return "Add contents of a given Reddit post dump to Elasticsearch."

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
        elasticsearch = establish_elasticsearch_connection(self._config)
        ensure_elasticsearch_index_available(REDDIT_INDEX)

        _LOGGER.info(f"Indexing Reddit dump file '{self._args.file}'...")

        debug_dynamic_mapping_difference(REDDIT_INDEX, RedditPost)

        _num_success, num_failed = bulk(
            elasticsearch,
            _bulk_helper(self._args.file),
            stats_only=True,
            raise_on_error=True,
            raise_on_exception=True,
            max_retries=5,
        )
        if num_failed:
            _LOGGER.error("Indexing failed.")
        else:
            _LOGGER.info("Indexing successful.")


def _bulk_helper(file: Path) -> Iterator[Mapping[str, object]]:
    for post_dict in load_reddit_dicts_from_dump(file):
        post = RedditPost.from_dict(post_dict)

        # Deserialize data and then serialize again. Needed so that our Python
        # conversion of some data types arrives in the JSON send to ElasticSearch.
        post.full_clean()

        yield post.to_dict(include_meta=True)
