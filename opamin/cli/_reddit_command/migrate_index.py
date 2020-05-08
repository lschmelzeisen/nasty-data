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
from typing import Sequence

from overrides import overrides
from typing_extensions import Final

from ..._util.elasticsearch import establish_elasticsearch_connection
from ...data.reddit import migrate_reddit_index
from .._command import Command

LOGGER: Final[Logger] = getLogger(__name__)


class MigrateIndexRedditCommand(Command):
    @classmethod
    @overrides
    def command(cls) -> str:
        return "migrate-index"

    @classmethod
    @overrides
    def aliases(cls) -> Sequence[str]:
        return ["mi", "migrate"]

    @classmethod
    @overrides
    def description(cls) -> str:
        return (
            "Creates a new Reddit Elasticsearch index with current "
            "settings and mappings, updates the index alias."
        )

    @classmethod
    @overrides
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        g = argparser.add_argument_group(
            "Migrate Arguments", "Control how the index is migrated."
        )
        g.add_argument(
            "-m",
            "--move-data",
            action="store_true",
            help="Reindex data from previous index into new one.",
        )

    @overrides
    def run(self) -> None:
        establish_elasticsearch_connection(self._config)

        LOGGER.info("Migrating Reddit index settings and mappings to new index.")
        migrate_reddit_index(move_data=self._args.move_data)
        # TODO: implement monitoring the reindexation for progress. See:
        #  https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html#docs-reindex-task-api
