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
import sys
from argparse import ArgumentParser
from collections import defaultdict
from json import JSONDecodeError
from pathlib import Path
from typing import Generator, List

from elasticsearch.exceptions import NotFoundError
from elasticsearch.helpers import bulk
from tqdm import tqdm

from opamin._util.compression import DecompressingTextIOWrapper
from opamin.commands.command import Command
from opamin.data.reddit import RedditPost, configure_reddit_index, reddit_index
from opamin.util.elasticsearch import connect_elasticsearch


class CommandReddit(Command):
    command: str = "reddit"
    aliases: List[str] = ["r"]
    description: str = "Operations related to Reddit data."

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    def run(self) -> None:
        pass


class CommandRedditDeleteIndex(CommandReddit):
    command: str = "delete-index"
    aliases: List[str] = ["di", "del"]
    description: str = "Delete the Elasticsearch Reddit index."

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    def run(self) -> None:
        connect_elasticsearch(self.config)

        self.logger.debug("Deleting Elasticsearch Reddit index ...")
        try:
            reddit_index.delete()
        except NotFoundError:
            self.logger.warn("No Elasticsearch Reddit index to delete.")
        self.logger.debug("Done.")


class CommandRedditConfigureIndex(CommandReddit):
    command: str = "configure-index"
    aliases: List[str] = ["ci", "conf"]
    description: str = (
        "Create the Elasticsearch Reddit index if necessary and configure mappings and "
        "other settings."
    )

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    def run(self) -> None:
        connect_elasticsearch(self.config)

        self.logger.debug("Configuring Elasticsearch Reddit index...")
        configure_reddit_index()
        self.logger.debug("Done.")


class CommandRedditIndexFile(CommandReddit):
    command: str = "index-file"
    aliases: List[str] = ["if", "index"]
    description: str = "Add all Reddit posts in a given file to " "Elasticsearch index."

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        argparser.add_argument(
            "file",
            metavar="<file>",
            type=Path,
            help="JSON-File containing Reddit posts to " "index.",
        )

    def run(self) -> None:
        elasticsearch_connection = connect_elasticsearch(self.config)

        if not reddit_index.exists():
            self.logger.error(
                "No Elasticsearch Reddit index exists. Make sure "
                'to create it with "reddit configure-index" '
                "first."
            )
            sys.exit()

        self.logger.debug(
            'Adding Reddit posts from file "{}" to Elasticsearch '
            "index...".format(self.args.file)
        )
        result = bulk(
            elasticsearch_connection,
            (
                d.to_dict(include_meta=True, skip_empty=True)
                for d in self.load_posts_from_file(self.args.file)
            ),
        )
        self.logger.debug("Result: {}".format(result))
        self.logger.debug("Done.")

    def load_posts_from_file(self, file: Path) -> Generator[RedditPost, None, None]:
        post_loading_stats = defaultdict(int)

        with DecompressingTextIOWrapper(file, encoding="utf-8") as fin, tqdm(
            total=fin.size(), unit="B", unit_scale=True, unit_divisor=1024
        ) as progress_bar:
            for line_no, line in enumerate(fin):
                progress_bar.n = fin.tell()
                progress_bar.refresh()

                # For some reason, there is at least one line (specifically,
                # line 29876 in file RS_2011-01.bz2) that contains NUL
                # characters at the beginning of it, which we remove with
                # the following.
                line = line.lstrip("\0")

                try:
                    obj = json.loads(line)

                    # The following is used for some manually created test
                    # files for development.
                    if "_comment" in obj:
                        continue

                    post = RedditPost.load_pushshift_json(obj)
                    post_loading_stats["Success."] += 1
                    yield post
                except (JSONDecodeError, RedditPost.LoadingError) as e:
                    error = str(e)
                    if isinstance(e, JSONDecodeError):
                        error = "Could not decode JSON."
                    post_loading_stats[error] += 1

                    non_logging_errors = [
                        RedditPost.IncompleteDataError,
                        RedditPost.PromotedContentError,
                    ]
                    if type(e) not in non_logging_errors:
                        self.logger.exception(
                            '{:s} From line {:d} in file "{}": '
                            "{:s}".format(error, line_no, file, line.rstrip("\n"))
                        )

        self.logger.debug("Statistics Reddit post loading:")
        total = sum(post_loading_stats.values())
        for error in sorted(post_loading_stats.keys()):
            count = post_loading_stats[error]
            self.logger.debug(
                "- {:s}: {:d} ({:.2%})".format(error[:-1], count, count / total)
            )
