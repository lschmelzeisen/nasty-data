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
from logging import getLogger
from pathlib import Path
from typing import Iterator, Mapping, Optional, Tuple

from elasticsearch_dsl import Date, InnerDoc, Integer, Keyword, Nested, Object
from nasty_utils import ColoredBraceStyleAdapter, DecompressingTextIOWrapper

from nasty_data.document.twitter import TwitterDocument

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


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


class NastyBatchResultsTwitterDocument(TwitterDocument):
    nasty_batch_meta = Nested(NastyBatchMeta)

    @classmethod
    def meta_field(cls) -> Tuple[str, str]:
        return "nasty_batch_meta", "id"


def load_document_dicts_from_nasty_batch_results(
    data_file: Path,
    *,
    progress_bar: bool = True,
) -> Iterator[Mapping[str, object]]:
    meta_file = data_file.with_name(
        data_file.name[: -len(".data.jsonl.xz")] + ".meta.json"
    )

    nasty_batch_meta: Optional[Mapping[str, object]] = None
    if meta_file.exists():
        with meta_file.open(encoding="UTF-8") as fin:
            nasty_batch_meta = json.load(fin)

    with DecompressingTextIOWrapper(
        data_file, encoding="UTF-8", progress_bar=progress_bar, warn_uncompressed=False
    ) as fin:
        for line_no, line in enumerate(fin):
            try:
                document_dict = json.loads(line)
            except JSONDecodeError:
                _LOGGER.error("Error in line {} of file '{}'.", line_no, data_file)
                raise

            document_dict["nasty_batch_meta"] = nasty_batch_meta
            yield document_dict
