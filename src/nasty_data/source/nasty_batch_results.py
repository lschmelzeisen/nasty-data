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
from json import JSONDecodeError
from logging import Logger, getLogger
from pathlib import Path
from typing import Iterator, Mapping, Optional

from elasticsearch_dsl import Date, InnerDoc, Integer, Keyword, Object
from nasty_utils import DecompressingTextIOWrapper
from typing_extensions import Final

from nasty_data.index.twitter import Tweet

_LOGGER: Final[Logger] = getLogger(__name__)


def load_dict_from_nasty_batch_results(
    data_file: Path,
) -> Iterator[Mapping[str, object]]:
    meta_file = data_file.with_name(
        data_file.name[: -len(".data.jsonl.xz")] + ".meta.json"
    )

    nasty_batch_meta: Optional[Mapping[str, object]] = None
    if meta_file.exists():
        with meta_file.open(encoding="UTF-8") as fin:
            nasty_batch_meta = json.load(fin)

    with DecompressingTextIOWrapper(
        data_file, encoding="UTF-8", progress_bar=True
    ) as fin:
        for line_no, line in enumerate(fin):
            try:
                result = json.loads(line)
            except JSONDecodeError:
                _LOGGER.error(f"Error in line {line_no} of file '{data_file}'.")
                raise

            result["nasty_batch_meta"] = nasty_batch_meta
            yield result


class NastyRequestMeta(InnerDoc):
    type = Keyword()
    query = Keyword()
    since = Date()
    until = Date()
    filter = Keyword()
    lang = Keyword()
    max_tweets = Integer()
    batch_size = Integer()
    tweet_id = Keyword()


class NastyBatchMeta(InnerDoc):
    id = Keyword()
    request = Object(NastyRequestMeta)
    completed_at = Date()


class NastyBatchResultsTweet(Tweet):
    nasty_batch_meta = Object(NastyBatchMeta)
