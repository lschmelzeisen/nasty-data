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

from __future__ import annotations

from datetime import datetime
from typing import Mapping, Type, TypeVar, Union, cast

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document
from elasticsearch_dsl import Index as EsIndex
from elasticsearch_dsl import (
    Keyword,
    MetaField,
    Search,
    Text,
    analyzer,
    connections,
    token_filter,
    tokenizer,
)
from overrides import overrides
from typing_extensions import Final

INDEX_ALIAS: Final[str] = "reddit"
INDEX_OPTIONS: Final[str] = "offsets"
INDEX_PHRASES: Final[bool] = False


def ensure_reddit_index_available() -> None:
    if not EsIndex(INDEX_ALIAS).exists():
        raise Exception("Reddit Index does not exist. Run: opamin reddit migrate-index")


def migrate_reddit_index(move_data: bool = True) -> None:
    """Creates a new Index with current mapping settings.

    The index is versioned by including the current timestamp in its name. Through this,
    existing previous indices with potentially incompatible mappings will not be
    affected. An alias is pointed to the newest index.

    Implements the alias migration pattern, based on:
    https://github.com/elastic/elasticsearch-dsl-py/blob/9b1a39dd47e8678bc4885b03b138293e189471d0/examples/alias_migration.py

    :param move_data: If true, reindex all data from the previous index to the new one
        (before updating the alias).
    """

    new_index_name = INDEX_ALIAS + "-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    RedditPost.init(index=new_index_name)

    new_index = EsIndex(new_index_name)
    all_indices = EsIndex(INDEX_ALIAS + "-*")

    if move_data:
        # TODO: test if this works and what happens if no previous index exists.
        connections.get_connection().reindex(
            body={"source": {"index": INDEX_ALIAS}, "dest": {"index": new_index_name}},
            request_timeout=3600,
            # TODO: find out if timeout works for large index
            # TODO: check if parameter name is actually `request_timeout` and not
            #  `timeout` as indicated by source.
        )
        new_index.refresh()

    if all_indices.exists_alias(name=INDEX_ALIAS):
        all_indices.delete_alias(name=INDEX_ALIAS)
    new_index.put_alias(name=INDEX_ALIAS)


standard_uax_url_email_analyzer = analyzer(
    "standard_uax_url_email",
    tokenizer=tokenizer("uax_url_email"),
    filter=[token_filter("asciifolding"), token_filter("lowercase")],
)
english_uax_url_email_analyzer = analyzer(
    "english_uax_url_email",
    tokenizer=tokenizer("uax_url_email"),
    filter=[
        token_filter("asciifolding"),
        token_filter(
            "english_possessive_stemmer", type="stemmer", language="possessive_english"
        ),
        token_filter("lowercase"),
        token_filter("english_stop", type="stop", stopwords="_english_"),
        token_filter("english_stemmer", type="stemmer", language="english"),
    ],
)


_T_RedditPost = TypeVar("_T_RedditPost", bound="RedditPost")


class RedditPost(Document):
    """Base class for all Reddit post (both submission and comments).

    Both submissions and comments are kept together in a single Elasticsearch index,
    although only a small number of fields is shared between both types. This was done
    out of gut instinct to make it easy to search both types with a single query.
    Some thorough experimentation on whether two indices improve storage or search
    performance might be interesting.

    The Elasticsearch join datatype is not used to link comments to submissions.
    This was done so that we can add comments to the index even if the corresponding
    submission has not been added (yet).
    """

    type_ = Keyword(required=True)
    author = Keyword(required=True)

    class Meta:
        # Disable dynamic addition of fields, so that we get errors if we try to submit
        # documents with fields not included in the mapping.
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic.html
        dynamic = MetaField("strict")

    class Index:
        name = INDEX_ALIAS
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "codec": "best_compression",
        }
        analyzers = [standard_uax_url_email_analyzer, english_uax_url_email_analyzer]

    def __init__(self, *args: object, **kwargs: object):
        if type(self) == RedditPost:
            raise TypeError(
                "Class RedditPost is not directly instantiable. Use one of its "
                "subclasses RedditSubmission or RedditComment."
            )
        super().__init__(*args, **kwargs)

    @classmethod
    @overrides
    def _matches(cls, hit: Mapping[str, object]) -> bool:
        """Checks if a search hit can be converted to this class or a subclass."""
        if not cast(str, hit["_index"]).startswith(INDEX_ALIAS + "-"):
            return False
        elif cls == RedditPost:
            return True
        return cast(Mapping[str, object], hit["_source"])["type_"] == cls.__name__

    @classmethod
    @overrides
    def from_es(cls, hit: Mapping[str, object]) -> RedditPost:
        """Convert a search hit to the corresponding (sub)class."""
        if cls == RedditPost:
            source = cast(Mapping[str, object], hit["_source"])
            type_ = cast(str, source["type_"])
            subcls = {subcls.__name__: subcls for subcls in cls.__subclasses__()}[type_]
            return subcls.from_es(hit)
        return super().from_es(hit)

    @classmethod
    @overrides
    def search(
        cls: Type[_T_RedditPost],
        using: Union[None, str, Elasticsearch] = None,
        index: Union[None, str, EsIndex] = None,
    ) -> Search[_T_RedditPost]:
        """Only return hits convertible to the respective subclass in search queries."""
        if cls == RedditPost:
            return super().search(using=using, index=index)
        return (
            super().search(using=using, index=index).filter("term", type_=cls.__name__)
        )


class RedditSubmission(RedditPost):
    title = Text(
        required=True,
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )


class RedditComment(RedditPost):
    body = Text(
        required=True,
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )
