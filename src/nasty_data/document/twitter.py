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
from typing import Dict, Mapping, MutableMapping, Optional, Sequence, cast

from elasticsearch_dsl import (
    Boolean,
    Date,
    Float,
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
from overrides import overrides
from typing_extensions import Final

from nasty_data.elasticsearch.index import BaseDocument

# Tweet objects are fairly well documented, see the following and its subpages:
# https://developer.twitter.com/en/docs/Twitters/data-dictionary/overview/
#
# However, Twitter returns different Tweet-JSONs depending on which API was used to
# obtain them. For this implementation, we only consider:
# - Twitter API GET statuses/lookup, i.e.,
#   https://developer.twitter.com/en/docs/tweets/post-and-engage/api-reference/get-statuses-lookup
#   with extended mode JSON rendering, i.e.,
#   https://developer.twitter.com/en/docs/tweets/Twitter-updates#extended-mode-json-rendering
# - The current NASTY-implementation, i.e., https://github.com/lschmelzeisen/nasty
# TODO: only NASTY tested so far.

# TODO: doc id_str over id

_INDEX_OPTIONS: Final[str] = "offsets"
_INDEX_PHRASES: Final[bool] = False
_INDEX_TERM_VECTOR: Final[str] = "yes"

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


class TwitterJsonAsStr(Keyword):
    # TODO: also overwrite serialize
    # TODO: document the different usages of mediaStatnds
    #   "mediaStats": { "r": { "missing": null }, "ttl": -1 }
    #   "mediaStats": { "r": "Missing", "ttl": -1 }
    #   and coordinate
    @overrides
    def _deserialize(self, data: object) -> str:
        return json.dumps(data)


# -- TwitterExtensions -----------------------------------------------------------------


class TwitterExtensionsMediaStats(InnerDoc):
    r = TwitterJsonAsStr(doc_values=False, index=False)
    ttl = Integer(doc_values=False, index=False)


class TwitterExtensions(InnerDoc):
    mediaStats = Object(TwitterExtensionsMediaStats)  # noqa: N815


class TwitterExtensionsMediaColorPaletteRgb(InnerDoc):
    red = Short(doc_values=False, index=False)
    green = Short(doc_values=False, index=False)
    blue = Short(doc_values=False, index=False)


class TwitterExtensionsMediaColorPalette(InnerDoc):
    rgb = Object(TwitterExtensionsMediaColorPaletteRgb)
    percentage = Float(doc_values=False, index=False)


class TwitterExtensionsMediaColor(InnerDoc):
    palette = Nested(TwitterExtensionsMediaColorPalette)


# -- TwitterEntities ------------------------------------------------------------------


class TwitterEntitiesIndicesText(InnerDoc):
    indices = Short(doc_values=False, index=False, multi=True)
    text = Keyword(doc_values=False, index=False)


class TwitterEntitiesMediaRect(InnerDoc):
    x = Short(doc_values=False, index=False)
    y = Short(doc_values=False, index=False)
    h = Short(doc_values=False, index=False)
    w = Short(doc_values=False, index=False)


class TwitterEntitiesMediaOriginalInfo(InnerDoc):
    height = Short(doc_values=False, index=False)
    width = Short(doc_values=False, index=False)
    focus_rects = Nested(TwitterEntitiesMediaRect)


class TwitterEntitiesMediaSize(InnerDoc):
    h = Short(doc_values=False, index=False)
    w = Short(doc_values=False, index=False)
    resize = Keyword(doc_values=False, index=False)


class TwitterEntitiesMediaSizes(InnerDoc):
    thumb = Object(TwitterEntitiesMediaSize)
    large = Object(TwitterEntitiesMediaSize)
    medium = Object(TwitterEntitiesMediaSize)
    small = Object(TwitterEntitiesMediaSize)


class TwitterEntitiesMediaVideoInfoVariant(InnerDoc):
    bitrate = Integer()
    content_type = Keyword()
    url = Keyword(doc_values=False, index=False)


class TwitterEntitiesMediaVideoInfo(InnerDoc):
    aspect_ratio = Short(multi=True)
    duration_millis = Integer()
    variants = Nested(TwitterEntitiesMediaVideoInfoVariant)


class TwitterEntitiesMediaFeature(InnerDoc):
    faces = Nested(TwitterEntitiesMediaRect)


class TwitterEntitiesMediaFeatures(InnerDoc):
    small = Object(TwitterEntitiesMediaFeature)
    medium = Object(TwitterEntitiesMediaFeature)
    large = Object(TwitterEntitiesMediaFeature)
    orig = Object(TwitterEntitiesMediaFeature)


class TwitterEntitiesMediaExtMediaAvailability(InnerDoc):
    status = Keyword()
    reason = Keyword()


class TwitterEntitiesAdditionalMediaInfoCallToAction(InnerDoc):
    url = Keyword()


class TwitterEntitiesAdditionalMediaInfoCallToActions(InnerDoc):
    visit_site = Object(TwitterEntitiesAdditionalMediaInfoCallToAction)
    watch_now = Object(TwitterEntitiesAdditionalMediaInfoCallToAction)


class TwitterEntitiesAdditionalMediaInfo(InnerDoc):
    title = Keyword(doc_values=False, index=False)
    description = Keyword(doc_values=False, index=False)
    call_to_actions = Object(TwitterEntitiesAdditionalMediaInfoCallToActions)
    embeddable = Boolean()
    monetizable = Boolean()


class TwitterEntitiesMedia(InnerDoc):
    id = Long(doc_values=False, index=False)
    id_str = Keyword(doc_values=False, index=False)
    indices = Short(doc_values=False, index=False, multi=True)

    media_url = Keyword(doc_values=False, index=False)
    media_url_https = Keyword(doc_values=False, index=False)
    url = Keyword(doc_values=False, index=False)
    display_url = Keyword(doc_values=False, index=False)
    expanded_url = Keyword(doc_values=False, index=False)

    type = Keyword()
    original_info = Object(TwitterEntitiesMediaOriginalInfo)
    sizes = Object(TwitterEntitiesMediaSizes)
    source_status_id = Long(doc_values=False, index=False)
    source_status_id_str = Keyword()
    source_user_id = Long(doc_values=False, index=False)
    source_user_id_str = Keyword()
    video_info = Object(TwitterEntitiesMediaVideoInfo)
    features = Object(TwitterEntitiesMediaFeatures)  # {}?

    media_key = Keyword(doc_values=False, index=False)
    ext_media_availability = Object(TwitterEntitiesMediaExtMediaAvailability)
    ext_alt_text = Keyword(doc_values=False, index=False)
    ext_media_color = Object(TwitterExtensionsMediaColor)
    ext = Object(TwitterExtensions)
    additional_media_info = Object(TwitterEntitiesAdditionalMediaInfo)


class TwitterEntitiesUserMention(InnerDoc):
    id = Long(doc_values=False, index=False)
    id_str = Keyword()
    indices = Short(doc_values=False, index=False, multi=True)
    name = Keyword()
    screen_name = Keyword()


class TwitterEntitiesUrl(InnerDoc):
    url = Keyword()
    expanded_url = Keyword()
    display_url = Keyword()
    indices = Short(multi=True)


class TwitterEntities(InnerDoc):
    hashtags = Nested(TwitterEntitiesIndicesText)
    symbols = Nested(TwitterEntitiesIndicesText)
    user_mentions = Nested(TwitterEntitiesUserMention)
    urls = Nested(TwitterEntitiesUrl)
    media = Nested(TwitterEntitiesMedia)


# -- TwitterOther ---------------------------------------------------------------------


class TwitterCoordinates(InnerDoc):
    coordinates = TwitterJsonAsStr()
    type = Keyword()


class TwitterPlace(InnerDoc):
    attributes = Nested()  # For sample, always [] if it exists.
    bounding_box = Object(TwitterCoordinates)
    contained_within = Nested()  # For sample, always [] if it exists.
    country = Keyword()
    country_code = Keyword()
    full_name = Keyword()
    id = Keyword()
    name = Keyword()
    place_type = Keyword()
    url = Keyword(doc_values=False, index=False)


class TwitterQuotedStatusPermalink(InnerDoc):
    url = Keyword(doc_values=False, index=False)
    expanded = Keyword(doc_values=False, index=False)
    display = Keyword(doc_values=False, index=False)


class TwitterScopes(InnerDoc):
    place_ids = Keyword(multi=True)


class TwitterSelfThread(InnerDoc):
    id = Long(doc_values=False, index=False)
    id_str = Keyword(doc_values=False)


class TwitterExt(InnerDoc):
    cameraMoment = Object(TwitterExtensionsMediaStats)  # noqa: N815


# -- TwitterUser ----------------------------------------------------------------------


class TwitterUserEntities(InnerDoc):
    url = Object(TwitterEntities)
    description = Object(TwitterEntities)


class TwitterUserExt(InnerDoc):
    highlightedLabel = Object(TwitterExtensionsMediaStats)  # noqa: N815


class TwitterUser(InnerDoc):
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
    entities = Object(TwitterUserEntities)

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
    profile_banner_extensions = Object(TwitterExtensions)
    profile_banner_extensions_alt_text = Keyword(doc_values=False, index=False)
    profile_banner_extensions_media_availability = Keyword(
        doc_values=False, index=False
    )
    profile_banner_extensions_media_color = Object(TwitterExtensionsMediaColor)
    profile_banner_url = Keyword(doc_values=False, index=False)
    profile_image_extensions = Object(TwitterExtensions)
    profile_image_extensions_alt_text = Keyword(doc_values=False, index=False)
    profile_image_extensions_media_availability = Keyword(doc_values=False, index=False)
    profile_image_extensions_media_color = Object(TwitterExtensionsMediaColor)
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
    ext = Object(TwitterUserExt)
    is_lifeline_institution = Boolean()

    advertiser_account_type = Keyword()
    advertiser_account_service_levels = Keyword(multi=True)
    profile_interstitial_type = Keyword()
    business_profile_state = Keyword()

    require_some_consent = Boolean()


# -- Twitter ---------------------------------------------------------------------------


class TwitterDocument(BaseDocument):
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

    entities = Object(TwitterEntities)
    extended_entities = Object(TwitterEntities)

    source = Keyword()

    in_reply_to_status_id = Long(doc_values=False, index=False)
    in_reply_to_status_id_str = Keyword()
    in_reply_to_user_id = Long(doc_values=False, index=False)
    in_reply_to_user_id_str = Keyword()
    in_reply_to_screen_name = Keyword()
    geo = Object(
        TwitterCoordinates
    )  # But with coords reversed compared to other attrs?
    coordinates = Object(TwitterCoordinates)
    place = Object(TwitterPlace)
    contributors = Keyword(multi=True)  # For sample, always None.
    withheld_in_countries = Keyword(multi=True)
    is_quote_status = Boolean()
    quoted_status_id = Long(doc_values=False, index=False)
    quoted_status_id_str = Keyword()
    quoted_status_permalink = Object(TwitterQuotedStatusPermalink)

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
    # Therefore we represent this as a Keyword in Elasticsearch.
    card = TwitterJsonAsStr(doc_values=False, index=False)

    scopes = Object(TwitterScopes)
    lang = Keyword()
    supplemental_language = Keyword()
    self_thread = Object(TwitterSelfThread)
    ext = Object(TwitterExt)

    user = Object(TwitterUser)

    @classmethod
    def index_settings(cls) -> MutableMapping[str, object]:
        settings = super().index_settings()
        settings["index.mapping.nested_fields.limit"] = 100
        return settings

    @classmethod
    @overrides
    def prepare_doc_dict(
        cls, doc_dict: MutableMapping[str, object]
    ) -> MutableMapping[str, object]:
        result = super().prepare_doc_dict(doc_dict)

        result["_id"] = result["id_str"]

        # TODO: document this
        extended_entities = cast(
            Optional[Mapping[str, Sequence[Mapping[str, Dict[str, object]]]]],
            result.get("extended_entities"),
        )
        for media in (extended_entities or {}).get("media", []):
            additional_media_info = media.get("additional_media_info")
            if additional_media_info:
                additional_media_info.pop("source_user", None)

        return result
