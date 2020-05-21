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
from typing import Dict, Iterator, Mapping, Sequence, cast

from elasticsearch_dsl import (
    Boolean,
    Date,
    Document,
    Float,
    Index,
    InnerDoc,
    Integer,
    Keyword,
    Long,
    Nested,
    Object,
    Short,
    Text,
    analyzer,
    char_filter,
    token_filter,
    tokenizer,
)
from nasty_utils.io_ import DecompressingTextIOWrapper
from overrides import overrides
from typing_extensions import Final

# Tweet objects are fairly well documented, see the following and its subpages:
# https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/
#
# However, Twitter returns different Tweet-JSONs depending on which API was used to
# obtain them. For this implementation, we only consider:
# - Twitter API GET statuses/lookup, i.e.,
#   https://developer.twitter.com/en/docs/tweets/post-and-engage/api-reference/get-statuses-lookup
#   with extended mode JSON rendering, i.e.,
#   https://developer.twitter.com/en/docs/tweets/tweet-updates#extended-mode-json-rendering
# - The current NASTY-implementation, i.e., https://github.com/lschmelzeisen/nasty
# TODO: only NASTY tested so far.

# TODO: doc id_str over id

_LOGGER: Final[Logger] = getLogger(__name__)

TWITTER_INDEX = Index("opamin-twitter")

_INDEX_OPTIONS: Final[str] = "offsets"
_INDEX_PHRASES: Final[bool] = False
_INDEX_TERM_VECTOR: Final[str] = "no"

_STANDARD_ANALYZER = analyzer(
    "standard_uax_url_email",
    char_filter=[char_filter("html_strip")],
    tokenizer=tokenizer("uax_url_email"),
    filter=[token_filter("asciifolding"), token_filter("lowercase")],
)
_ENGLISH_ANALYZER = analyzer(
    "english_uax_url_email",
    char_filter=[char_filter("html_strip")],
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


class TweetJsonAsStr(Keyword):
    # TODO: also overwrite serialize
    # TODO: document the different usages of mediaStatnds
    #   "mediaStats": { "r": { "missing": null }, "ttl": -1 }
    #   "mediaStats": { "r": "Missing", "ttl": -1 }
    #   and coordinate
    @overrides
    def _deserialize(self, data: object) -> str:
        return json.dumps(data)


# -- TweetExtensions -------------------------------------------------------------------


class TweetExtensionsMediaStats(InnerDoc):
    r = TweetJsonAsStr(doc_values=False, index=False)
    ttl = Integer(doc_values=False, index=False)


class TweetExtensions(InnerDoc):
    mediaStats = Object(TweetExtensionsMediaStats)  # noqa: N815


class TweetExtensionsMediaColorPaletteRgb(InnerDoc):
    red = Short(doc_values=False, index=False)
    green = Short(doc_values=False, index=False)
    blue = Short(doc_values=False, index=False)


class TweetExtensionsMediaColorPalette(InnerDoc):
    rgb = Object(TweetExtensionsMediaColorPaletteRgb)
    percentage = Float(doc_values=False, index=False)


class TweetExtensionsMediaColor(InnerDoc):
    palette = Nested(TweetExtensionsMediaColorPalette)


# -- TweetEntities ---------------------------------------------------------------------


class TweetEntitiesIndicesText(InnerDoc):
    indices = Short(doc_values=False, index=False, multi=True)
    text = Keyword(doc_values=False, index=False)


class TweetEntitiesMediaRect(InnerDoc):
    x = Short(doc_values=False, index=False)
    y = Short(doc_values=False, index=False)
    h = Short(doc_values=False, index=False)
    w = Short(doc_values=False, index=False)


class TweetEntitiesMediaOriginalInfo(InnerDoc):
    height = Short(doc_values=False, index=False)
    width = Short(doc_values=False, index=False)
    focus_rects = Nested(TweetEntitiesMediaRect)


class TweetEntitiesMediaSize(InnerDoc):
    h = Short(doc_values=False, index=False)
    w = Short(doc_values=False, index=False)
    resize = Keyword(doc_values=False, index=False)


class TweetEntitiesMediaSizes(InnerDoc):
    thumb = Object(TweetEntitiesMediaSize)
    large = Object(TweetEntitiesMediaSize)
    medium = Object(TweetEntitiesMediaSize)
    small = Object(TweetEntitiesMediaSize)


class TweetEntitiesMediaVideoInfoVariant(InnerDoc):
    bitrate = Integer()
    content_type = Keyword()
    url = Keyword(doc_values=False, index=False)


class TweetEntitiesMediaVideoInfo(InnerDoc):
    aspect_ratio = Short(multi=True)
    duration_millis = Integer()
    variants = Nested(TweetEntitiesMediaVideoInfoVariant)


class TweetEntitiesMediaFeature(InnerDoc):
    faces = Nested(TweetEntitiesMediaRect)


class TweetEntitiesMediaFeatures(InnerDoc):
    small = Object(TweetEntitiesMediaFeature)
    medium = Object(TweetEntitiesMediaFeature)
    large = Object(TweetEntitiesMediaFeature)
    orig = Object(TweetEntitiesMediaFeature)


class TweetEntitiesMediaExtMediaAvailability(InnerDoc):
    status = Keyword()
    reason = Keyword()


class TweetEntitiesAdditionalMediaInfoCallToAction(InnerDoc):
    url = Keyword()


class TweetEntitiesAdditionalMediaInfoCallToActions(InnerDoc):
    visit_site = Object(TweetEntitiesAdditionalMediaInfoCallToAction)
    watch_now = Object(TweetEntitiesAdditionalMediaInfoCallToAction)


class TweetEntitiesAdditionalMediaInfo(InnerDoc):
    title = Keyword(doc_values=False, index=False)
    description = Keyword(doc_values=False, index=False)
    call_to_actions = Object(TweetEntitiesAdditionalMediaInfoCallToActions)
    embeddable = Boolean()
    monetizable = Boolean()


class TweetEntitiesMedia(InnerDoc):
    id = Long(doc_values=False, index=False)
    id_str = Keyword(doc_values=False, index=False)
    indices = Short(doc_values=False, index=False, multi=True)

    media_url = Keyword(doc_values=False, index=False)
    media_url_https = Keyword(doc_values=False, index=False)
    url = Keyword(doc_values=False, index=False)
    display_url = Keyword(doc_values=False, index=False)
    expanded_url = Keyword(doc_values=False, index=False)

    type = Keyword()
    original_info = Object(TweetEntitiesMediaOriginalInfo)
    sizes = Object(TweetEntitiesMediaSizes)
    source_status_id = Long(doc_values=False, index=False)
    source_status_id_str = Keyword()
    source_user_id = Long(doc_values=False, index=False)
    source_user_id_str = Keyword()
    video_info = Object(TweetEntitiesMediaVideoInfo)
    features = Object(TweetEntitiesMediaFeatures)  # {}?

    media_key = Keyword(doc_values=False, index=False)
    ext_media_availability = Object(TweetEntitiesMediaExtMediaAvailability)
    ext_alt_text = Keyword(doc_values=False, index=False)
    ext_media_color = Object(TweetExtensionsMediaColor)
    ext = Object(TweetExtensions)
    additional_media_info = Object(TweetEntitiesAdditionalMediaInfo)


class TweetEntitiesUserMention(InnerDoc):
    id = Long(doc_values=False, index=False)
    id_str = Keyword()
    indices = Short(doc_values=False, index=False, multi=True)
    name = Keyword()
    screen_name = Keyword()


class TweetEntitiesUrl(InnerDoc):
    url = Keyword()
    expanded_url = Keyword()
    display_url = Keyword()
    indices = Short(multi=True)


class TweetEntities(InnerDoc):
    hashtags = Nested(TweetEntitiesIndicesText)
    symbols = Nested(TweetEntitiesIndicesText)
    user_mentions = Nested(TweetEntitiesUserMention)
    urls = Nested(TweetEntitiesUrl)
    media = Nested(TweetEntitiesMedia)


# -- TweetOther ------------------------------------------------------------------------


class TweetCoordinates(InnerDoc):
    coordinates = TweetJsonAsStr()
    type = Keyword()


class TweetPlace(InnerDoc):
    attributes = Nested()  # For sample, always [] if it exists.
    bounding_box = Object(TweetCoordinates)
    contained_within = Nested()  # For sample, always [] if it exists.
    country = Keyword()
    country_code = Keyword()
    full_name = Keyword()
    id = Keyword()
    name = Keyword()
    place_type = Keyword()
    url = Keyword(doc_values=False, index=False)


class TweetQuotedStatusPermalink(InnerDoc):
    url = Keyword(doc_values=False, index=False)
    expanded = Keyword(doc_values=False, index=False)
    display = Keyword(doc_values=False, index=False)


class TweetScopes(InnerDoc):
    place_ids = Keyword(multi=True)


class TweetSelfThread(InnerDoc):
    id = Long(doc_values=False, index=False)
    id_str = Keyword(doc_values=False)


class TweetExt(InnerDoc):
    cameraMoment = Object(TweetExtensionsMediaStats)  # noqa: N815


# -- TweetUser -------------------------------------------------------------------------


class TweetUserEntities(InnerDoc):
    url = Object(TweetEntities)
    description = Object(TweetEntities)


class TweetUserExt(InnerDoc):
    highlightedLabel = Object(TweetExtensionsMediaStats)  # noqa: N815


class TweetUser(InnerDoc):
    id = Long(doc_values=False, index=False)
    id_str = Keyword()
    name = Keyword()
    screen_name = Keyword()
    location = Keyword()
    description = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        analyzer=_STANDARD_ANALYZER,
        fields={"english_analyzed": Text(analyzer=_ENGLISH_ANALYZER)},
        term_vector=_INDEX_TERM_VECTOR,
    )
    url = Keyword(doc_values=False)
    entities = Object(TweetUserEntities)

    protected = Boolean()
    followers_count = Integer()
    fast_followers_count = Integer()
    normal_followers_count = Integer()
    friends_count = Integer()
    listed_count = Integer()
    created_at = Date()
    favourites_count = Integer()
    utc_offset = Keyword()  # For sample, always None.
    time_zone = Keyword()  # For sample, always None.
    geo_enabled = Boolean()
    verified = Boolean()
    statuses_count = Integer()
    media_count = Integer()
    lang = Keyword()  # For sample, always None.
    contributors_enabled = Boolean()

    is_translator = Boolean()
    is_translation_enabled = Boolean()
    translator_type = Keyword()
    withheld_in_countries = Keyword(multi=True)

    profile_background_color = Keyword(doc_values=False, index=False)
    profile_background_image_url = Keyword(doc_values=False, index=False)
    profile_background_image_url_https = Keyword(doc_values=False, index=False)
    profile_background_tile = Boolean()
    profile_banner_extensions = Object(TweetExtensions)
    profile_banner_extensions_alt_text = Keyword(doc_values=False, index=False)
    profile_banner_extensions_media_availability = Keyword(
        doc_values=False, index=False
    )
    profile_banner_extensions_media_color = Object(TweetExtensionsMediaColor)
    profile_banner_url = Keyword(doc_values=False, index=False)
    profile_image_extensions = Object(TweetExtensions)
    profile_image_extensions_alt_text = Keyword(doc_values=False, index=False)
    profile_image_extensions_media_availability = Keyword(doc_values=False, index=False)
    profile_image_extensions_media_color = Object(TweetExtensionsMediaColor)
    profile_image_url = Keyword(doc_values=False, index=False)
    profile_image_url_https = Keyword(doc_values=False, index=False)
    profile_link_color = Keyword(doc_values=False, index=False)
    profile_sidebar_border_color = Keyword(doc_values=False, index=False)
    profile_sidebar_fill_color = Keyword(doc_values=False, index=False)
    profile_text_color = Keyword(doc_values=False, index=False)
    profile_use_background_image = Boolean()
    has_extended_profile = Boolean()
    default_profile = Boolean()
    default_profile_image = Boolean()
    pinned_tweet_ids = Long(doc_values=False, index=False, multi=True)
    pinned_tweet_ids_str = Keyword(doc_values=False, index=False, multi=True)

    has_custom_timelines = Boolean()

    # Probably need log in.
    can_dm = Boolean()  # For sample, always None.
    can_media_tag = Boolean()  # For sample, always None.
    following = Boolean()  # For sample, always None.
    follow_request_sent = Boolean()  # For sample, always None.
    notifications = Boolean()  # For sample, always None.
    muting = Boolean()  # For sample, always None.
    blocking = Boolean()  # For sample, always None.
    bocked_by = Boolean()  # For sample, always None.
    want_retweets = Boolean()  # For sample, always None.
    followed_by = Boolean()  # For sample, always None.
    ext = Object(TweetUserExt)
    is_lifeline_institution = Boolean()

    advertiser_account_type = Keyword()
    advertiser_account_service_levels = Keyword(multi=True)
    profile_interstitial_type = Keyword()
    business_profile_state = Keyword()

    require_some_consent = Boolean()


# -- Tweet -----------------------------------------------------------------------------


class Tweet(Document):
    class Index:
        name = TWITTER_INDEX._name
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "codec": "best_compression",
            # "index.mapping.total_fields.limit": 10000,
            "index.mapping.nested_fields.limit": 100,
        }
        analyzer = [_STANDARD_ANALYZER, _ENGLISH_ANALYZER]

    created_at = Date()

    id = Long(doc_values=False, index=False)
    id_str = Keyword(doc_values=False, index=False)

    full_text = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        analyzer=_STANDARD_ANALYZER,
        fields={"english_analyzed": Text(analyzer=_ENGLISH_ANALYZER)},
        term_vector=_INDEX_TERM_VECTOR,
    )

    truncated = Boolean()
    display_text_range = Integer(doc_values=False, index=False, multi=True)

    entities = Object(TweetEntities)
    extended_entities = Object(TweetEntities)

    source = Keyword()

    in_reply_to_status_id = Long(doc_values=False, index=False)
    in_reply_to_status_id_str = Keyword()
    in_reply_to_user_id = Long(doc_values=False, index=False)
    in_reply_to_user_id_str = Keyword()
    in_reply_to_screen_name = Keyword()
    geo = Object(TweetCoordinates)  # But with coords reversed compared to other attrs?
    coordinates = Object(TweetCoordinates)
    place = Object(TweetPlace)
    contributors = Keyword(multi=True)  # For sample, always None.
    withheld_in_countries = Keyword(multi=True)
    is_quote_status = Boolean()
    quoted_status_id = Long(doc_values=False, index=False)
    quoted_status_id_str = Keyword()
    quoted_status_permalink = Object(TweetQuotedStatusPermalink)

    retweet_count = Integer()
    favorite_count = Integer()
    reply_count = Integer()

    conversation_id = Long(doc_values=False, index=False)
    conversation_id_str = Keyword()

    favorited = Boolean()
    retweeted = Boolean()
    possibly_sensitive = Boolean()
    possibly_sensitive_editable = Boolean()

    # The Twitter card feature, i.e.,
    # https://developer.twitter.com/en/docs/tweets/optimize-with-cards/overview/abouts-cards
    # is fairly niche and would add a large complexity and many fields to the mapping.
    # Therefore we represent this as a Keyword in ElasticSearch.
    card = TweetJsonAsStr(doc_values=False, index=False)

    scopes = Object(TweetScopes)
    lang = Keyword()
    supplemental_language = Keyword()
    self_thread = Object(TweetSelfThread)
    ext = Object(TweetExt)

    user = Object(TweetUser)

    @classmethod
    def from_dict(cls, tweet_dict: Mapping[str, object]) -> "Tweet":
        tweet_dict = dict(tweet_dict)
        tweet_dict["_id"] = tweet_dict["id_str"]

        # TODO: document this
        extended_entities = cast(
            Mapping[str, Sequence[Mapping[str, Dict[str, object]]]],
            tweet_dict.get("extended_entities"),
        )
        for media in (extended_entities or {}).get("media", []):
            additional_media_info = media.get("additional_media_info")
            if additional_media_info:
                additional_media_info.pop("source_user", None)  # TODO: changes param.

        return cls(**tweet_dict)

    @classmethod
    @overrides
    def _matches(cls, hit: Mapping[str, object]) -> bool:
        if not cast(str, hit["_index"]).startswith(TWITTER_INDEX._name):
            return False
        return True


def load_tweet_dicts_from_dump(file: Path) -> Iterator[Mapping[str, object]]:
    with DecompressingTextIOWrapper(file, encoding="UTF-8", progress_bar=True) as fin:
        for line_no, line in enumerate(fin):
            try:
                yield json.loads(line)
            except JSONDecodeError:
                _LOGGER.error(f"Error in line {line_no} of file '{file}'.")
                raise
