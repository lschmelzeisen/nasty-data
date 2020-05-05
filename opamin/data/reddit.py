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

import json
from datetime import datetime
from json import JSONDecodeError
from logging import Logger, getLogger
from operator import itemgetter
from pathlib import Path
from typing import (
    Dict,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Boolean, Date, Document
from elasticsearch_dsl import Index as EsIndex
from elasticsearch_dsl import (
    InnerDoc,
    Integer,
    Keyword,
    MetaField,
    Nested,
    Object,
    Search,
    Text,
    analyzer,
    connections,
    token_filter,
    tokenizer,
)
from overrides import overrides
from typing_extensions import Final

from .._util.compression import DecompressingTextIOWrapper

LOGGER: Final[Logger] = getLogger(__name__)

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


class RedditLoadingError(Exception):
    """Raised when it was impossible to load a Post."""


class IncompleteDataError(RedditLoadingError):
    """Raised for posts where data is missing that was deemed required."""


class PromotedContentError(RedditLoadingError):
    """Raised for posts that are promoted content."""


_T_RedditPost = TypeVar("_T_RedditPost", bound="RedditPost")


class RedditAward(InnerDoc):
    id = Keyword(required=True)
    name = Keyword(required=True)
    count = Integer(required=True)
    award_type = Keyword(required=True)

    @classmethod
    def name_from_id(cls, award_id: str) -> str:
        if award_id == "gid_1":
            return "Silver"
        elif award_id == "gid_2":
            return "Gold"
        elif award_id == "gid_3":
            return "Platinum"
        else:
            raise ValueError(f"Unknown award id: '{award_id}'")


class RedditCollection(InnerDoc):
    author_id = Keyword(required=True)
    author_name = Keyword(required=True)
    collection_id = Keyword(required=True)
    created_at_utc = Date(required=True)
    description = Text(
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )
    display_layout = Keyword()
    last_update_utc = Date(required=True)
    link_ids = Keyword(required=True, multi=True)
    permalink = Keyword()
    subreddit_id = Keyword(required=True)
    title = Text(
        required=True,
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )


class RedditMediaOEmbed(InnerDoc):
    author_name = Keyword()
    author_url = Keyword()
    description = Text(
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )
    html = Keyword(index=False)
    provider_name = Keyword()
    provider_url = Keyword()
    thumbnail_url = Keyword()
    title = Text(
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )
    type_ = Keyword(required=True)
    url = Keyword()


class RedditMediaRedditVideo(InnerDoc):
    dash_url = Keyword()
    duration = Integer()
    fallback_url = Keyword()
    hls_url = Keyword()
    scrubber_media_url = Keyword()


class RedditMedia(InnerDoc):
    type_ = Keyword()  # Only exists for oembed (not for reddit_video)
    oembed = Object(RedditMediaOEmbed)
    reddit_video = Object(RedditMediaRedditVideo)


class RedditMediaMetadata(InnerDoc):
    id = Keyword(required=True)
    e = Keyword(required=True)
    m = Keyword()
    s = Keyword()
    t = Keyword()
    dash_url = Keyword()
    hls_url = Keyword()


class RedditPost(Document):
    """Base class for all Reddit post (both links and comments).

    Both links and comments are kept together in a single Elasticsearch index,
    although only a small number of fields is shared between both types. This was done
    out of gut instinct to make it easy to search both types with a single query.
    Some thorough experimentation on whether two indices improve storage or search
    performance might be interesting.

    The Elasticsearch join datatype is not used to link comments to links.
    This was done so that we can add comments to the index even if the corresponding
    links has not been added (yet).
    """

    type_ = Keyword(required=True)

    created_utc = Date(required=True)
    edited = Date()
    retrieved_on = Date(required=True)

    author = Keyword(required=True)
    author_flair_text = Keyword()
    distinguished = Keyword()

    all_awardings = Nested(RedditAward)

    score = Integer()

    subreddit = Keyword(required=True)
    subreddit_id = Keyword(required=True)
    subreddit_type = Keyword()

    media_metadata = Nested(RedditMediaMetadata)

    archived = Boolean(required=True)
    stickied = Boolean(required=True)
    locked = Boolean(required=True)
    quarantine = Boolean(required=True)

    permalink = Keyword(required=True)

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
                "Do not instantiate RedditPost directly. Use one of its subclasses:"
                + ", ".join(subcls.__name__ for subcls in type(self).__subclasses__())
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


class RedditLink(RedditPost):
    title = Text(
        required=True,
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )
    selftext = Text(
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )
    domain = Keyword(required=True)
    url = Keyword(required=True)
    link_flair_text = Keyword()

    collections = Nested(RedditCollection)

    media = Object(RedditMedia)
    thumbnail = Keyword()

    brand_safe = Boolean()
    contest_mode = Boolean(required=True)
    is_self = Boolean(required=True)
    is_original_content = Boolean()
    is_video = Boolean(required=True)
    over_18 = Boolean(required=True)
    spoiler = Boolean(required=True)

    crosspost_parent = Keyword()
    num_comments = Integer()
    num_crossposts = Integer()
    post_hint = Keyword()
    suggested_sort = Keyword()

    event_is_live = Boolean()
    event_start = Date()
    event_end = Date()

    whitelist_status = Keyword()
    parent_whitelist_status = Keyword()


class RedditComment(RedditPost):
    link_id = Keyword(required=True)
    parent_id = Keyword(required=True)
    body = Text(
        required=True,
        index_options=INDEX_OPTIONS,
        index_phrases=INDEX_PHRASES,
        analyzer=standard_uax_url_email_analyzer,
        fields={"english_analyzed": Text(analyzer=english_uax_url_email_analyzer)},
    )
    controversiality = Integer(required=True)
    is_submitter = Boolean()
    collapsed = Boolean()
    collapsed_reason = Keyword()


def parse_reddit_dict(post_dict: Mapping[str, object]) -> RedditPost:
    # Only tested with Pushshift data.

    if "promoted" in post_dict:
        raise PromotedContentError()

    # We do want to modify the caller's dict. Therefore we take a shallow copy of the
    # dict here. In sub-functions we will do the same for child dicts. We do this
    # instead of a deep copy here so that in each sub-function we also have original and
    # modified dict available.
    post_dict, orig_post_dict = dict(post_dict), post_dict

    # We want to verify that this code is aware of all the different values the Reddit-
    # JSON object contains. Therefore we remove each attribute we handle from post_dict
    # with one of the following idioms and can thus check in the end which attributes
    # we have not handled:
    # - `post_dict.pop("attr")` for attributes that always exist.
    # - `post_dict.pop("attr", None)` for attributes that sometimes exist (and
    #   substitute with default value).
    # - `post_dict.pop("attr") or None` to convert False-like values to a default value.

    # Main objective of this function: construct dict to be send to ElasticSearch.
    es_post: Dict[str, object] = {}

    _parse_reddit_dict_id(post_dict, es_post)
    _parse_reddit_dict_time(post_dict, es_post)
    _parse_reddit_dict_author(post_dict, es_post)
    _parse_reddit_dict_awards(post_dict, es_post)
    _parse_reddit_dict_score(post_dict, es_post)
    _parse_reddit_dict_subreddit(post_dict, es_post)
    _parse_reddit_dict_media_metadata(post_dict, es_post)
    _parse_reddit_dict_misc(post_dict, es_post)
    _parse_reddit_dict_log_in_required(post_dict)
    _parse_reddit_dict_moderator_required(post_dict)

    cls: Type[RedditPost]
    if "title" in post_dict:
        cls = RedditLink
        _parse_reddit_dict_link(post_dict, es_post)
        _parse_reddit_dict_link_collections(post_dict, es_post)
        _parse_reddit_dict_link_media(post_dict, es_post)
        _parse_reddit_dict_link_misc(post_dict, es_post)
        _parse_reddit_dict_link_whitelist_status(post_dict, es_post)
        _parse_reddit_dict_link_fix_odd_data(es_post)
    elif "body" in post_dict:
        cls = RedditComment
        _parse_reddit_dict_comment(post_dict, es_post)
    else:
        raise IncompleteDataError(
            "Could not determine whether given post is link or comment."
        )

    # Make "permalink" into a full link for convenience (especially from Kibana).
    assert isinstance(es_post["permalink"], str) and es_post["permalink"]
    es_post["permalink"] = "https://www.reddit.com/" + es_post["permalink"]

    # TODO: replace these with actual logging / error code
    if post_dict:
        from pprint import pprint

        print("post_dict:")
        pprint(post_dict)

    return cls(**es_post)


def _parse_reddit_dict_id(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # The Reddit internal ID, will be prefixed with type prefix later.
    es_post["_id"] = post_dict.pop("id")
    assert isinstance(es_post["_id"], str) and es_post["_id"]

    # "name" is "id" with type prefix (e.g., "t1_" for comments).
    if "name" in post_dict:
        assert cast(str, post_dict.pop("name")).endswith(es_post["_id"])


def _parse_reddit_dict_time(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # The UTC time the post was created.
    created_utc = datetime.utcfromtimestamp(
        int(cast(Union[str, int], post_dict.pop("created_utc")))
    )
    assert created_utc >= datetime(year=2005, month=6, day=23)  # Reddit founding date.
    es_post["created_utc"] = created_utc

    # Local time the post was created (deprecated).
    post_dict.pop("created", None)

    # If the post has been edited, edit date in UTC time. For some old edited
    # comments, this was instead a bool, we substitute with post creation time.
    if "edited" in post_dict:
        edited = post_dict.pop("edited")
        if isinstance(edited, bool):
            if edited:
                es_post["edited"] = es_post["created_utc"]
            else:
                es_post["edited"] = datetime.utcfromtimestamp(
                    cast(Union[int, float], edited)
                )

    # When the post was retrieved. If not-existent, substitute post creation time.
    if "retrieved_on" in post_dict:
        es_post["retrieved_on"] = datetime.utcfromtimestamp(
            cast(int, post_dict.pop("retrieved_on"))
        )
    else:
        es_post["retrieved_on"] = es_post["created_utc"]


def _parse_reddit_dict_author(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # The Reddit username of the author of the link/comment.
    es_post["author"] = post_dict.pop("author")
    assert isinstance(es_post["author"], str)
    assert es_post["author"]

    # author_created_utc and author_fullname seem to be introduced into dumps
    # starting 2018-06 but even then are not always included.
    post_dict.pop("author_created_utc", None)
    post_dict.pop("author_fullname", None)

    # The text for the author's flair (None or a non-empty string).
    es_post["author_flair_text"] = post_dict.pop("author_flair_text") or None

    # Ignore other flair related stuff.
    post_dict.pop("author_flair_background_color", None)
    post_dict.pop("author_flair_css_class", None)
    post_dict.pop("author_flair_richtext", None)
    post_dict.pop("author_flair_template_id", None)
    post_dict.pop("author_flair_text_color", None)
    post_dict.pop("author_flair_type", None)

    # Ignore other author stuff.
    post_dict.pop("author_premium", False)  # Dependant on crawl-date.
    post_dict.pop("author_cakeday", False)  # Dependant on crawl-date.
    post_dict.pop("author_patreon_flair", False)  # No idea what this is (always False).

    # Whether author is moderator or admin.
    es_post["distinguished"] = post_dict.pop("distinguished")
    assert es_post["distinguished"] in [None, "moderator", "admin"]


def _parse_reddit_dict_awards(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    es_all_awardings = []

    # Modern way to specify Reddit awards.
    all_awardings = cast(
        Sequence[Dict[str, object]], post_dict.pop("all_awardings", [])
    )
    if all_awardings:
        for award in all_awardings:
            assert isinstance(award["count"], int) and award["count"]
            es_all_awardings.append(
                {
                    "id": award["id"],
                    "name": award["name"],
                    "count": award["count"],
                    "award_type": award["award_type"],
                }
                # Ignoring all other award properties.
            )

    # Legacy way to specify only Platinum/Gold/Silver awards.
    gildings = cast(Mapping[str, int], post_dict.pop("gildings", {}))
    if gildings:
        if es_all_awardings:  # If newer attribute also exists and is parsed already.
            # Zero entries seem to be messing in this case.
            for award_id, award_count in gildings.items():
                assert any(
                    award["id"] == award_id and award["count"] == award_count
                    for award in es_all_awardings
                )
        else:
            for award_id, award_count in gildings.items():
                assert isinstance(award_count, int) != 0
                if award_count == 0:
                    continue
                es_all_awardings.append(
                    {
                        "id": award_id,
                        "name": RedditAward.name_from_id(award_id),
                        "count": award_count,
                        "award_type": "global",
                    }
                )

    # Legacy way to specify only gold awards.
    gilded = post_dict.pop("gilded", 0)
    if gilded:
        legacy_award_id = "gid_2"
        if es_all_awardings:  # If newer attribute also exists.
            assert any(
                award["id"] == legacy_award_id and award["count"] == gilded
                for award in es_all_awardings
            )
        else:
            es_all_awardings = [
                {
                    "id": legacy_award_id,
                    "name": RedditAward.name_from_id(legacy_award_id),
                    "count": gilded,
                    "award_type": "global",
                }
            ]

    # Duplicate total counter of other forms award specification.
    total_awards_received = post_dict.pop("total_awards_received", None)
    if total_awards_received:
        assert total_awards_received == sum(
            award["count"] for award in es_all_awardings
        )

    es_all_awardings.sort(key=itemgetter("id"))
    es_post["all_awardings"] = es_all_awardings

    # Whether or not this link can be "gilded" by giving the link author Reddit
    # gold (decided not useful).
    post_dict.pop("can_gild", None)

    # Not sure what these are.
    post_dict.pop("associated_award", None)
    post_dict.pop("awarders", None)


def _parse_reddit_dict_score(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # Whether the post's score was visible at the time of retrieving. If this is
    # the case, the score will default to 1. Here we elect to not save the score
    # for these cases. For more information on this, see:
    # https://www.reddit.com/r/AskReddit/comments/1dfnku/why_are_comment_scores_hidden/
    if post_dict.pop("score_hidden", False):
        assert post_dict.pop("score") == 1
    else:
        # The net-score of the post (sometimes not present).
        es_post["score"] = post_dict.pop("score")
        assert es_post["score"] is None or isinstance(es_post["score"], int)

    # Direct access of up- and down-votes (deprecated, not available for newer posts).
    post_dict.pop("ups", None)
    post_dict.pop("downs", None)


def _parse_reddit_dict_subreddit(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # The name of the subreddit the post belongs to.
    es_post["subreddit"] = post_dict.pop("subreddit")
    assert isinstance(es_post["subreddit"], str) and es_post["subreddit"]

    # The name of the subreddit prefixed with "r/".
    post_dict.pop("subreddit_name_prefixed", None)

    # The Reddit Fullname of the subreddit.
    es_post["subreddit_id"] = post_dict.pop("subreddit_id")
    assert isinstance(es_post["subreddit_id"], str) and es_post["subreddit_id"]

    # The type of subreddit.
    es_post["subreddit_type"] = post_dict.pop("subreddit_type", None)
    assert es_post["subreddit_type"] in [
        None,
        "archived",
        "gold_only",
        "private",
        "public",
        "restricted",
        "user",
    ]

    # Number of subscribers of the subreddit (only for links and dependant on
    # retrieval time).
    post_dict.pop("subreddit_subscribers", None)


def _parse_reddit_dict_media_metadata(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # Information about all referenced media in a link's selftext or a comment's
    # body, e.g., included images or videos. Only for images/videos hosted on
    # Reddit.
    media_metadata = cast(
        Optional[Mapping[str, Mapping[str, object]]],
        post_dict.pop("media_metadata", None),
    )

    es_media_metadata = []
    for media_metadatum in dict(media_metadata or {}).values():
        media_metadatum = dict(media_metadatum)  # Don't manipulate original dict.
        s = cast(Mapping[str, str], media_metadatum.pop("s", {}))

        es_media_metadatum = {}

        # Reddit internal ID of media.
        es_media_metadatum["id"] = media_metadatum.pop("id")
        assert isinstance(es_media_metadatum["id"], str) and es_media_metadatum["id"]

        # Type of media.
        es_media_metadatum["e"] = media_metadatum.pop("e")
        assert es_media_metadatum["e"] in ["Image", "AnimatedImage", "RedditVideo"]

        # For Image and AnimatedImage:
        # MIME-Type of image.
        es_media_metadatum["m"] = media_metadatum.pop("m", None)
        assert es_media_metadatum["m"] in [
            None,  # TODO: needed?
            "image/png",
            "image/jpg",
            "image/gif",
        ]
        # URL for Image. MP4s sometimes exists for GIFs.
        es_media_metadatum["s"] = s.get("u") or s.get("gif") or s.get("mp4")
        for k in s.keys():
            # Assert that there are no values in "s" that we do not know about.
            assert k in ["u", "gif", "mp4", "x", "y"]
        # Special tag for image.
        es_media_metadatum["t"] = media_metadatum.pop("t", None)
        assert es_media_metadatum["t"] in [None, "sticker", "emoji"]

        # For RedditVideo:
        es_media_metadatum["dash_url"] = media_metadatum.pop("dashUrl", None)
        es_media_metadatum["hls_url"] = media_metadatum.pop("hlsUrl", None)

        # Not sure what these are.
        media_metadatum.pop("status")  # Always "valid" for Pushshift.
        media_metadatum.pop("isGif", None)
        media_metadatum.pop("x", None)
        media_metadatum.pop("y", None)

        if media_metadatum:
            from pprint import pprint

            print("media_metadatum:")
            pprint(media_metadatum)

        es_media_metadata.append(es_media_metadatum)

    es_media_metadata.sort(key=itemgetter("id"))
    es_post["media_metadata"] = es_media_metadata


def _parse_reddit_dict_misc(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # If the post has been archived.
    es_post["archived"] = post_dict.pop("archived", False)

    # If the post is set as the sticky in its subreddit/thread.
    es_post["stickied"] = post_dict.pop("stickied", False)

    # If the post has been locked by a moderator.
    es_post["locked"] = post_dict.pop("locked", False)

    # If this post is part of a quarantined subreddit.
    # "quarantine" for links, "quarantined" for comments.
    es_post["quarantine"] = post_dict.pop("quarantine", False)
    es_post["quarantine"] = post_dict.pop("quarantined", False) or es_post["quarantine"]

    # No idea what these are.
    post_dict.pop("can_mod_post", None)
    post_dict.pop("no_follow", None)
    post_dict.pop("removal_reason", None)
    post_dict.pop("send_replies", None)
    # Only present on comments.
    post_dict.pop("mod_note", None)
    post_dict.pop("mod_reason_by", None)
    post_dict.pop("mod_reason_title", None)

    # Not exactly sure what this is. If present it's values are either "markdown"
    # or "richtext". Was used from 2005-06 to 20010-12 and recently again with the
    # redesign from 2010-10 to 2018-10.
    post_dict.pop("rte_mode", None)


def _parse_reddit_dict_log_in_required(post_dict: Dict[str, object]) -> None:
    # Whether or not the user has saved the post.
    post_dict.pop("saved", None)

    # Whether the user likes (upvoted) the link or not
    post_dict.pop("likes", None)


def _parse_reddit_dict_moderator_required(post_dict: Dict[str, object]) -> None:
    # Whether the post has been approved by a moderator.
    post_dict.pop("approved", None)
    # The UTC time the post was approved by a moderator.
    post_dict.pop("approved_at_utc", None)
    # The Moderator who approved the post.
    post_dict.pop("approved_by", None)

    # The UTC time the link was banned by a post.
    post_dict.pop("banned_at_utc", None)
    # The username of the moderator who banned the post.
    post_dict.pop("banned_by", None)
    # The note left when banning the post.
    post_dict.pop("banned_note", None)

    # An array of reports made by Moderators on this post.
    post_dict.pop("mod_reports", None)
    # The number of reports the link has.
    post_dict.pop("num_reports", None)
    # A string array containing report reasons supplied by users.
    post_dict.pop("report_reasons", None)
    # A collection of reports made against this post by other users.
    post_dict.pop("user_reports", None)
    # Reports of mod-helpers. See:
    # https://www.reddit.com/r/ModSupport/comments/d31dim/what_is_the_steward_reports_field_on_the/
    post_dict.pop("steward_reports", None)


def _parse_reddit_dict_link(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    es_post["type_"] = RedditLink.__name__

    # Assemble partial ID into Reddit fullname.
    es_post["_id"] = "t3_" + cast(str, es_post["_id"])

    # The permalink to this link.
    es_post["permalink"] = post_dict.pop("permalink")
    assert es_post["permalink"]

    # The title of the link post.
    es_post["title"] = post_dict.pop("title")
    assert es_post["title"]

    # The text of a self-post (may be empty, which we convert to None).
    selftext = post_dict.pop("selftext")
    if selftext:
        es_post["selftext"] = (
            cast(str, selftext)
            # Need to escape exactly these characters.
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&amp;", "&")
        )
    post_dict.pop("selftext_html", None)  # Decide to ignore.

    # The domain of the URL the link was submitted for.
    # If it starts with "self." it is eather a self-post or a crosspost.
    es_post["domain"] = post_dict.pop("domain")
    assert es_post["domain"]

    # The url of the link.
    es_post["url"] = post_dict.pop("url")
    assert es_post["url"]

    # The text for the link's flair (may be None).
    es_post["link_flair_text"] = post_dict.pop("link_flair_text")

    # Ignore other flair related stuff.
    post_dict.pop("link_flair_background_color", None)
    post_dict.pop("link_flair_css_class", None)
    post_dict.pop("link_flair_richtext", None)
    post_dict.pop("link_flair_template_id", None)
    post_dict.pop("link_flair_text_color", None)
    post_dict.pop("link_flair_type", None)


def _parse_reddit_dict_link_misc(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # This is true if Reddit has determined the subreddit the Link was posted in is safe
    # for advertising.
    es_post["brand_safe"] = post_dict.pop("brand_safe", None)

    # If true, the link has been set to Contest mode. See:
    # https://www.reddit.com/r/bestof2012/comments/159bww/introducing_contest_mode_a_tool_for_your_voting/
    es_post["contest_mode"] = post_dict.pop("contest_mode", False)

    # Whether or not the link is a self-post.
    es_post["is_self"] = post_dict.pop("is_self")

    # Not sure what this is.
    es_post["is_original_content"] = post_dict.pop("is_original_content", None)
    if cast(datetime, es_post["created_utc"]) >= datetime(2018, 8, 3):
        assert es_post["is_original_content"] is not None

    # Whether or not the link is a video post (i.e., using v.redd.it).
    es_post["is_video"] = post_dict.pop("is_video", False)

    # Whether or not the link is NSFW.
    es_post["over_18"] = post_dict.pop("over_18", False)

    # Whether or not the link has been marked as a spoiler.
    es_post["spoiler"] = post_dict.pop("spoiler", False)

    # Fullname of the post this post is a crosspost to.
    es_post["crosspost_parent"] = post_dict.pop("crosspost_parent", None)
    # Full JSON of the post this post is a crosspost to. Not added into
    # Elasticsearch to keep things simpler.
    post_dict.pop("crosspost_parent_list", None)

    # The number of comments the link has (includes removed comments).
    es_post["num_comments"] = post_dict.pop("num_comments", None)

    # The number of crossposts the link has (not sure on this).
    es_post["num_crossposts"] = post_dict.pop("num_crossposts", None)

    # Info on what kind of link this is (availability seems to be subreddit
    # dependant).
    es_post["post_hint"] = post_dict.pop("post_hint", None)
    assert es_post["post_hint"] in [
        None,
        "self",
        "image",
        "hosted:video",
        "link",
        "rich:video",
        "video",
    ]

    # The suggested sort order for comments made to the link.
    es_post["suggested_sort"] = post_dict.pop("suggested_sort", None)
    assert es_post["suggested_sort"] in [
        None,
        "new",
        "top",
        "qa",
        "confidence",
        "old",
        "controversial",
        "random",
        "live",
    ]

    # Data related to the new event system. See:
    # https://www.reddit.com/r/modnews/comments/bgibu9/an_update_on_making_it_easier_to_host_events_on/
    es_post["event_is_live"] = post_dict.pop("event_is_live", None)
    event_start = post_dict.pop("event_start", None)
    if event_start:
        es_post["event_start"] = datetime.utcfromtimestamp(cast(int, event_start))
    event_end = post_dict.pop("event_end", None)
    if event_end:
        es_post["event_end"] = datetime.utcfromtimestamp(cast(int, event_end))

    # Decided to ignore.
    post_dict.pop("pinned", None)  # Dependant on retrieval time.
    post_dict.pop("media_only", None)  # Equivalent to media and no selftext.
    post_dict.pop("is_reddit_media_domain", None)  # Is domain "i.redd.it"/"v.redd.it"?

    # Log-in required.
    post_dict.pop("hidden", None)  # Whether the link has been hidden by the user.
    post_dict.pop("clicked", None)  # Whether the link has been clicked by the user.
    post_dict.pop("visited", None)  # Whether the user has visited the link.

    # Moderator required.
    post_dict.pop("ban_note", None)  # Always None if it exists.
    post_dict.pop("hide_score", None)  # Always False if it exists.
    post_dict.pop("ignore_reports", None)  # Always False if it exists.
    post_dict.pop("removed", None)  # Always False if it exists.
    post_dict.pop("spam", None)  # Always False if it exists.

    # No idea what these are.
    post_dict.pop("allow_live_comments", None)
    post_dict.pop("category", None)
    post_dict.pop("content_categories", None)
    post_dict.pop("from", None)  # Always None if it exists.
    post_dict.pop("from_id", None)  # Always None if it exists.
    post_dict.pop("from_kind", None)  # Always None if it exists.
    post_dict.pop("discussion_type", None)  # Always None if it exists.
    post_dict.pop("is_crosspostable", None)
    post_dict.pop("is_meta", None)  # Always False if it exists.
    post_dict.pop("is_robot_indexable", None)
    post_dict.pop("post_categories", None)  # Always None if it exists.
    post_dict.pop("previous_visits", None)  # List of Timestamps if it exists.
    post_dict.pop("view_count", None)  # Always None if it exists.


def _parse_reddit_dict_link_collections(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    es_collections = []

    for collection in cast(
        Sequence[Mapping[str, object]], post_dict.pop("collections", [])
    ):
        collection = dict(collection)

        es_collection = {}

        # Data related to the new collection functionality. See:
        # https://mods.reddithelp.com/hc/en-us/articles/360027311431-Collections

        # Fullname of the collection's author.
        es_collection["author_id"] = collection.pop("author_id")
        assert (
            isinstance(es_collection["author_id"], str) and es_collection["author_id"]
        )

        # Username of the collection's author.
        es_collection["author_name"] = collection.pop("author_name")
        assert (
            isinstance(es_collection["author_name"], str)
            and es_collection["author_name"]
        )

        # Fullname of the collection.
        es_collection["collection_id"] = collection.pop("collection_id")
        assert (
            isinstance(es_collection["collection_id"], str)
            and es_collection["collection_id"]
        )

        # Timestamp the collection was created.
        es_collection["created_at_utc"] = datetime.utcfromtimestamp(
            cast(float, collection.pop("created_at_utc"))
        )

        # Plain-text description of the collection.
        es_collection["description"] = collection.pop("description", None)

        # Not sure.
        es_collection["display_layout"] = collection.pop("display_layout", None)
        assert es_collection["display_layout"] in [None, "TIMELINE", "GALLERY"]

        # Timestamp the collection was last updated.
        es_collection["last_update_utc"] = datetime.utcfromtimestamp(
            cast(float, collection.pop("last_update_utc"))
        )

        # List of all link fullnames that are included in this collection.
        es_collection["link_ids"] = collection.pop("link_ids")
        assert isinstance(es_collection["link_ids"], Sequence)
        for link_id in es_collection["link_ids"]:
            assert isinstance(link_id, str) and link_id
        assert es_post["_id"] in es_collection["link_ids"]

        # Permalink to the collection
        permalink = cast(Optional[str], collection.pop("permalink", None))
        if permalink:
            es_collection["permalink"] = "https://www.reddit.com" + permalink

        # Fullname of the subreddit the collection is in.
        es_collection["subreddit_id"] = collection.pop("subreddit_id")
        assert (
            isinstance(es_collection["subreddit_id"], str)
            and es_collection["subreddit_id"]
        )

        # Title of the collection.
        es_collection["title"] = collection.pop("title")
        assert isinstance(es_collection["title"], str) and es_collection["title"]

        if collection:
            from pprint import pprint

            print("collection:")
            pprint(collection)

        es_collections.append(es_collection)

    es_collections.sort(key=itemgetter("collection_id"))
    es_post["collections"] = es_collections


def _parse_reddit_dict_link_media(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # Info for when the post links to embeddable content or info on the first
    # embeddable content from the post if it is a selftext.
    # secure_media seems to contain the exact same stuff as media but is guaranteed
    # to be https, so we prefer that.
    media = post_dict.pop("media")
    media = post_dict.pop("secure_media", None) or media
    media = dict(cast(Mapping[str, object], media) or {})

    if media:
        if "oembed" in media:
            es_post["media"] = _from_reddit_dict_link_media_oembed(media)
        elif "reddit_video" in media:
            es_post["media"] = _from_reddit_dict_link_media_reddit_video(media)
        else:
            raise IncompleteDataError("Could not determine given media format.")

    if media:
        from pprint import pprint

        print("media:")
        pprint(media)

    # Full url to the thumbnail for the post.
    es_post["thumbnail"] = post_dict.pop("thumbnail", None) or "default"
    if not cast(str, es_post["thumbnail"]).startswith("http"):
        assert es_post["thumbnail"] in [
            "default",
            "image",
            "nsfw",
            "self",
            "spoiler",
        ]

    # These only seem to contain redundant information from (secure_) media.
    post_dict.pop("media_embed")
    post_dict.pop("secure_media_embed", None)

    # Decided not to need these.
    post_dict.pop("preview", None)
    post_dict.pop("thumbnail_width", None)
    post_dict.pop("thumbnail_height", None)


def _from_reddit_dict_link_media_oembed(
    media: Dict[str, object]
) -> Mapping[str, object]:
    oembed = dict(cast(Mapping[str, object], media.pop("oembed")))

    es_oembed = {}

    # The following varies from service to service where the media is from.
    # E.g.: YouTube account name.
    es_oembed["author_name"] = oembed.pop("author_name", None)
    # E.g.: YouTube account url.
    es_oembed["author_url"] = oembed.pop("author_url", None)
    # E.g.: description of YouTube video.
    es_oembed["description"] = oembed.pop("description", None)
    # E.g.: iframe to embed the YouTube video.
    es_oembed["html"] = oembed.pop("html")
    es_oembed["html"] = oembed.pop("html5", None) or es_oembed["html"]
    assert es_oembed["html"]
    # E.g.: "YouTube"
    es_oembed["provider_name"] = oembed.pop("provider_name")
    assert es_oembed["provider_name"]
    # E.g.: "http://www.youtube.com/"
    es_oembed["provider_url"] = oembed.pop("provider_url")
    assert es_oembed["provider_url"]
    # E.g.: YouTube-Thumbnail of video.
    es_oembed["thumbnail_url"] = oembed.pop("thumbnail_url", None)
    # E.g.: Title of YouTube video.
    es_oembed["title"] = oembed.pop("title", None)
    # E.g.: "youtube.com"
    es_oembed["type_"] = oembed.pop("type")
    assert es_oembed["type_"]
    # E.g.: URL to the YouTube video.
    es_oembed["url"] = oembed.pop("url", None)

    # Decided not to store.
    oembed.pop("width", None)
    oembed.pop("height", None)
    oembed.pop("thumbnail_width", None)
    oembed.pop("thumbnail_height", None)

    # No idea what these are.
    oembed.pop("version")  # Always "1.0" for Pushshift data.
    oembed.pop("cache_age", None)
    oembed.pop("mean_alpha", None)
    oembed.pop("thumbnail_size", None)

    if oembed:
        from pprint import pprint

        print("oembed:")
        pprint(oembed)

    return {
        "oembed": es_oembed,
        "type_": media.pop("type"),
    }


def _from_reddit_dict_link_media_reddit_video(
    media: Dict[str, object]
) -> Mapping[str, object]:
    reddit_video = dict(cast(Dict[str, object], media.pop("reddit_video")))

    es_reddit_video = {}

    # Not sure what these URLs are.
    es_reddit_video["dash_url"] = reddit_video.pop("dash_url")
    es_reddit_video["duration"] = reddit_video.pop("duration")
    es_reddit_video["fallback_url"] = reddit_video.pop("fallback_url")
    es_reddit_video["hls_url"] = reddit_video.pop("hls_url")
    es_reddit_video["scrubber_media_url"] = reddit_video.pop("scrubber_media_url")

    for value in es_reddit_video:
        assert value

    # Decided not to store.
    reddit_video.pop("width")
    reddit_video.pop("height")

    # No idea what these are.
    reddit_video.pop("is_gif")
    reddit_video.pop("transcoding_status")  # Always "completed" for Pushshift data.

    if reddit_video:
        from pprint import pprint

        print("reddit_video:")
        pprint(reddit_video)

    return {
        "reddit_video": es_reddit_video,
    }


def _parse_reddit_dict_link_whitelist_status(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    # Not exactly sure what these are for.
    for attr in ["whitelist_status", "parent_whitelist_status"]:
        es_post[attr] = post_dict.pop(attr, None)
        assert es_post[attr] in [
            None,
            "all_ads",
            "house_only",
            "no_ads",
            "promo_adult",
            "promo_adult_nsfw",
            "promo_all",
            "promo_specified",
        ]

    # These seem to be shorthands for the two above introduced around 2018-03.
    post_dict.pop("wls", None)
    post_dict.pop("pwls", None)


def _parse_reddit_dict_link_fix_odd_data(es_post: Dict[str, object]) -> None:
    # In rare old cases we have a URL like "http://self.quickquestions".
    if cast(str, es_post["domain"]).startswith("self.") and cast(
        str, es_post["url"]
    ).endswith(cast(str, es_post["domain"])):
        es_post["url"] = "https://www.reddit.com" + cast(str, es_post["permalink"])
        es_post["is_self"] = True

    # In rare cases the URL does not start with "http".
    if not cast(str, es_post["url"]).lower().startswith("http"):
        es_post["url"] = "https://www.reddit.com" + cast(str, es_post["url"])

    # TODO: fix odd domain data to have correct domain of url
    #   e.g. it can be "/r/Seaofthieves/comments/8nncoj/drunken_drums_are_best_drums/"


def _parse_reddit_dict_comment(
    post_dict: Dict[str, object], es_post: Dict[str, object]
) -> None:
    es_post["type_"] = RedditComment.__name__

    # Assemble partial ID into Reddit fullname.
    es_post["_id"] = "t1_" + cast(str, es_post["_id"])

    # Fullname of the link this comment is in.
    es_post["link_id"] = post_dict.pop("link_id")
    assert isinstance(es_post["link_id"], str) and es_post["link_id"]

    # Fullname of the thing this comment is a reply to, or the link in it.
    es_post["parent_id"] = post_dict.pop("parent_id")
    assert isinstance(es_post["parent_id"], str) and es_post["parent_id"]

    # The permalink to this comment (only available reliably since 2017-11).
    es_post["permalink"] = post_dict.pop("permalink_url", None)
    es_post["permalink"] = post_dict.pop("permalink", None) or es_post["permalink"]
    if not es_post["permalink"]:
        # If it does not exist, construct it.
        es_post["permalink"] = "r/{}/comments/{}/_/{}".format(
            es_post["subreddit"], es_post["link_id"][3:], cast(str, es_post["_id"])[:3]
        )

    # The body of the comment.
    es_post["body"] = post_dict.pop("body")
    assert isinstance(es_post["body"], str) and es_post["body"]

    # HTML format of the comment body (only very rarely available).
    post_dict.pop("body_html", None)

    # The controversiality score of the comment.
    es_post["controversiality"] = post_dict.pop("controversiality")
    assert isinstance(es_post["controversiality"], int)

    # Whether the comment author is also the link author (available since mid 2017-07).
    es_post["is_submitter"] = post_dict.pop("is_submitter", None)

    # Whether the comment is collapsed initially (available since mid 2017-07).
    es_post["collapsed"] = post_dict.pop("collapsed", None)

    # Why the comment has been collapsed (available since mid 2017-07 but most often
    # it is `None` even if available).
    es_post["collapsed_reason"] = post_dict.pop("collapsed_reason", None)
    assert es_post["collapsed_reason"] in [
        None,
        "may be sensitive content",
        "comment score below threshold",
    ]

    # No idea what this is.
    post_dict.pop("collapsed_because_crowd_control", None)

    # A collection of child comments for this comment (always the empty string for
    # Pushshift data).
    post_dict.pop("replies", None)


def load_reddit_dicts_from_dump(file: Path) -> Iterator[Mapping[str, object]]:
    with DecompressingTextIOWrapper(file, encoding="UTF-8", progress_bar=True) as fin:
        for line_no, line in enumerate(fin):
            # For some reason, there is at least one line (specifically,
            # line 29876 in file RS_2011-01.bz2) that contains NUL
            # characters at the beginning of it, which we remove with
            # the following.
            line = line.lstrip("\0")

            try:
                yield json.loads(line)
            except JSONDecodeError:
                LOGGER.error(f"Error in line {line_no} of file '{file}'.")
                raise


def load_reddit_posts_from_dump(
    file: Path, skip_promoted: bool = True
) -> Iterator[RedditPost]:
    for post_dict in load_reddit_dicts_from_dump(file):
        try:
            yield parse_reddit_dict(post_dict)
        except PromotedContentError:
            if not skip_promoted:
                raise
