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

from datetime import date, datetime
from typing import Any, Mapping, MutableMapping, Union, cast

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
)
from overrides import overrides
from typing_extensions import Final

from nasty_data.elasticsearch_.index import BaseDocument
from nasty_utils import checked_cast

# This file contains the elasticsearch-dsl mapping for reading and writing Reddit posts
# (specifically from the Pushshift dumps).
#
# It is important to note that the data format quite a bit over the years. Additionally,
# it is very poorly documented. The following are the only sensible sources I could
# find:
# - https://www.reddit.com/dev/api/
# - https://github.com/reddit-archive/reddit/wiki/JSON
# - https://psraw.readthedocs.io/en/develop/Module/about_RedditLink/
# - https://psraw.readthedocs.io/en/latest/Module/about_RedditComment/
#
# I first invested considerable time into trying to unify the data format (i.e., convert
# legacy fields to their newer counter-parts, ident), but ultimately failed. This was,
# because the dataset is extremely large (currently 777 GB) and it is therefore
# practically impossible to take a development subset from it that contains all relevant
# field / value pairs. Additionally, one has to realize that the data format will
# probably change again in the future, and reindexing the whole dataset again would be
# prohibitively expensive.
#
# Finally, I therefore went with the alternative of trying to ingest the data as
# unmodified as possible and instead do all data unification during read access. Through
# this, I should be able to avoid having to ever reindex the whole dataset again. And
# because the data is then already indexed, figuring out which fields contain which
# values under which circumstances can be done much faster and thoroughly. Of course,
# if you are going to have very frequent read access at some point in time doing the
# field unification before indexing would be beneficial. However, for my use case of
# academic analysis I don't foresee this to become the case.
#
# One other important thing is to keep the index size from exploding. While I wanted to
# retain as much flexibility in usage later down the line (because I don't quite know
# what we would want to do with the data later on) I had to make a few sacrifices:
# - I did opt for "best_compression", i.e., DEFLATE, for the index instead of the
#   default, i.e. LZ4. While this should increase access times a bit it considerably
#   decreases storage space requirements, and we don't anticipate frequent access
#   anyhow.
# - Post IDs are stored as Elasticsearch IDs so that we don't need to keep track of two
#   different IDs in code that uses this. One direct consequence of this is, because the
#   Pushshift dataset contains posts with the same IDs multiple times in rare cases,
#   that we can only store one version of each post with the same ID.
# - For analyzers we were again guided by our intended use case of extracting arguments:
#   recall is much more important for us then precision and we basically only care about
#   English text. For this reason we perform asciifolding and add a second analyzer that
#   performs English stemming. Additionally, since Reddit texts may contain URLs or
#   Email addresses we used the uax_url_email tokenizer instead of the standard one.
#   Last, because we don't anticipate building an actual user facing UI for our copy of
#   the Reddit dataset, we didn't pay any attention to stuff like autocompletion. One
#   note for the future: adding more analyzers does not seem to require that much more
#   storage space, because only the inverted index is enlarged and analyzer values are
#   not stored per entry. For the storage space / functionality trade-off again, we set
#   index_options='offsets' and don't enable 'index_phrases' (see experiment below).
#   Additionally, we store term vectors for the most important
#   Text-fields, but not for every Text field because they need considerable space.
# - I also briefly experimented with disabling _source and enabling "store", but this
#   did not yield a very noticeable change in index size but severely reduces stuff we
#   can do with the data.

# TODO: Data reading has not been implemented yet. Do so! Take old code as reference:
#   https://github.com/lschmelzeisen/opamin/blob/03b35d4005fc1642662e69672de4cd2d4ca4660e/opamin/data/reddit.py


_INDEX_OPTIONS: Final[str] = "offsets"
_INDEX_PHRASES: Final[bool] = False
_INDEX_TERM_VECTOR: Final[str] = "with_positions_offsets"


class RedditDate(Date):
    def __init__(
        self, millis: bool = False, *args: Any, **kwargs: Any,
    ):
        self._millis = millis
        super().__init__(*args, **kwargs)

    @overrides
    def _deserialize(self, data: object) -> Union[datetime, date]:
        # In some cases, fields which are nowadays only for dates were a bool earlier.
        # We represent those bools with special date values.
        if isinstance(data, bool):
            # 0001-01-01 is lowest representable date in Python.
            return date(1, 1, 1 + int(data))

        # In some cases, fields which are dates (i.e., integers) are stored as strings.
        if isinstance(data, str):
            data = int(data)

        if isinstance(data, int) or isinstance(data, float):
            if self._millis:
                # Divide by a float to preserve milliseconds on the datetime.
                data /= 1000.0
            return datetime.utcfromtimestamp(data)

        return super()._deserialize(data)


class RedditFlairRichtext(InnerDoc):
    a = Keyword()
    e = Keyword()
    t = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    u = Keyword(doc_values=False, index=False)


class RedditAwardingResizedIcon(InnerDoc):
    height = Short(doc_values=False, index=False)
    url = Keyword(doc_values=False, index=False)
    width = Short(doc_values=False, index=False)


class RedditAwarding(InnerDoc):
    award_type = Keyword()
    coin_price = Integer()
    coin_reward = Integer()
    count = Integer()
    days_of_drip_extension = Integer()
    days_of_premium = Integer()
    description = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    end_date = RedditDate()
    icon_height = Short(doc_values=False, index=False)
    icon_url = Keyword(doc_values=False, index=False)
    icon_width = Short(doc_values=False, index=False)
    id = Keyword()
    is_enabled = Boolean()
    name = Keyword()
    resized_icons = Nested(RedditAwardingResizedIcon)
    start_date = RedditDate()
    subreddit_coin_reward = Integer()
    subreddit_id = Keyword()


class RedditGildings(InnerDoc):
    gid_1 = Integer()
    gid_2 = Integer()
    gid_3 = Integer()


class RedditMediaMetadataS(InnerDoc):
    u = Keyword(doc_values=False, index=False)
    x = Short(doc_values=False, index=False)
    y = Short(doc_values=False, index=False)
    gif = Keyword(doc_values=False, index=False)
    mp4 = Keyword(doc_values=False, index=False)


class RedditMediaMetadata(InnerDoc):
    dashUrl = Keyword(doc_values=False, index=False)  # noqa: N815
    e = Keyword()
    hlsUrl = Keyword(doc_values=False, index=False)  # noqa: N815
    id = Keyword(doc_values=False)
    isGif = Boolean()  # noqa: N815
    m = Keyword()
    s = Object(RedditMediaMetadataS)
    status = Keyword()
    t = Keyword()
    x = Short(doc_values=False, index=False)
    y = Short(doc_values=False, index=False)


class RedditLinkMediaOEmbed(InnerDoc):
    author_name = Keyword()
    author_url = Keyword()
    cache_age = Long(doc_values=False, index=False)
    description = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    height = Short(doc_values=False, index=False)
    html = Keyword(doc_values=False, index=False)
    html5 = Keyword(doc_values=False, index=False)
    mean_alpha = Float(doc_values=False, index=False)
    provider_name = Keyword()
    provider_url = Keyword()
    thumbnail_height = Short(doc_values=False, index=False)
    thumbnail_url = Keyword(doc_values=False, index=False)
    thumbnail_size = Short(doc_values=False, index=False)
    thumbnail_width = Short(doc_values=False, index=False)
    title = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    type = Keyword()
    version = Keyword()
    url = Keyword()
    width = Short(doc_values=False, index=False)


class RedditLinkMediaRedditVideo(InnerDoc):
    dash_url = Keyword(doc_values=False, index=False)
    duration = Integer()
    fallback_url = Keyword(doc_values=False, index=False)
    height = Short(doc_values=False, index=False)
    hls_url = Keyword(doc_values=False, index=False)
    is_gif = Boolean()
    scrubber_media_url = Keyword(doc_values=False, index=False)
    transcoding_status = Keyword()
    width = Boolean()


class RedditLinkMedia(InnerDoc):
    content = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    event_id = Keyword()
    height = Short(doc_values=False, index=False)
    oembed = Object(RedditLinkMediaOEmbed)
    reddit_video = Object(RedditLinkMediaRedditVideo)
    type = Keyword()
    width = Short(doc_values=False, index=False)


class RedditLinkMediaEmbed(InnerDoc):
    content = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    height = Short(doc_values=False, index=False)
    media_domain_url = Keyword(doc_values=False, index=False)
    scrolling = Boolean()
    width = Short(doc_values=False, index=False)


class RedditLinkPreviewImageResolution(InnerDoc):
    height = Short(doc_values=False, index=False)
    url = Keyword(doc_values=False, index=False)
    width = Short(doc_values=False, index=False)


class RedditLinkPreviewImageVariant(InnerDoc):
    resolutions = Nested(RedditLinkPreviewImageResolution)
    source = Object(RedditLinkPreviewImageResolution)


class RedditLinkPreviewImageVariants(InnerDoc):
    gif = Object(RedditLinkPreviewImageVariant)
    mp4 = Object(RedditLinkPreviewImageVariant)
    nsfw = Object(RedditLinkPreviewImageVariant)
    obfuscated = Object(RedditLinkPreviewImageVariant)


class RedditLinkPreviewImage(InnerDoc):
    id = Keyword(doc_values=False, index=False)
    resolutions = Nested(RedditLinkPreviewImageResolution)
    source = Object(RedditLinkPreviewImageResolution)
    variants = Object(RedditLinkPreviewImageVariants)


class RedditLinkPreview(InnerDoc):
    enabled = Boolean()
    images = Nested(RedditLinkPreviewImage)
    reddit_video_preview = Object(RedditLinkMediaRedditVideo)


class RedditLinkCollection(InnerDoc):
    author_id = Keyword()
    author_name = Keyword()
    collection_id = Keyword(doc_values=False)
    created_at_utc = RedditDate()
    description = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    display_layout = Keyword()
    last_update_utc = RedditDate()
    link_ids = Keyword(multi=True, doc_values=False)
    permalink = Keyword(doc_values=False, index=False)
    subreddit_id = Keyword()
    title = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )


class RedditLinkOutboundLink(InnerDoc):
    created = RedditDate(millis=True)
    expiration = RedditDate(millis=True)
    url = Keyword(doc_values=False, index=False)


class RedditBaseDocument(BaseDocument):
    id = Keyword(doc_values=False, index=False)
    name = Keyword(doc_values=False, index=False)
    permalink = Keyword(doc_values=False, index=False)

    # Would like to set doc_values=False on `created` but the following Kibana bug
    # prevents this: https://github.com/elastic/kibana/issues/11179
    created = RedditDate(index=False)
    created_utc = RedditDate()
    edited = RedditDate()
    retrieved_on = RedditDate()

    author = Keyword()
    author_cakeday = Boolean()
    author_created_utc = RedditDate()
    author_flair_background_color = Keyword(doc_values=False, index=False)
    author_flair_css_class = Keyword()
    author_flair_richtext = Nested(RedditFlairRichtext)
    author_flair_template_id = Keyword()
    author_flair_text = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    author_flair_text_color = Keyword(doc_values=False, index=False)
    author_flair_type = Keyword()
    author_fullname = Keyword()
    author_id = Keyword()
    author_patreon_flair = Keyword()
    author_premium = Boolean()
    distinguished = Keyword()

    all_awardings = Nested(RedditAwarding)
    associated_award = Keyword()  # For sample, only None if it exists.
    awarders = Nested()  # For sample, only [] if it exists.
    can_gild = Boolean()
    gildings = Object(RedditGildings)
    gilded = Integer()
    total_awards_received = Integer()

    downs = Integer()
    score = Integer()
    score_hidden = Boolean()
    ups = Integer()

    subreddit = Keyword()
    subreddit_name_prefixed = Keyword()
    subreddit_id = Keyword()
    subreddit_subscribers = Integer()
    subreddit_type = Keyword()

    media_metadata = Nested(RedditMediaMetadata)

    archived = Boolean()
    stickied = Boolean()
    locked = Boolean()

    rte_mode = Keyword()

    saved = Boolean()  # For sample, always False if it exists.
    likes = Integer()  # For sample, always None if it exists.

    mod_note = Keyword()  # For sample, always None if it exists.
    mod_reason_by = Keyword()  # For sample, always None if it exists.
    mod_reason_title = Keyword()  # For sample, always None if it exists.
    approved = Boolean()
    approved_at_utc = RedditDate()  # For sample, always None if it exists.
    approved_by = Keyword()  # For sample, always None if it exists.
    banned_at_utc = RedditDate()  # For sample, always None if it exists.
    banned_by = Keyword()  # For sample, always None if it exists.
    ban_note = Keyword()  # For sample, always None if it exists.
    mod_reports = Nested()  # For sample, always [] if it exists.
    num_reports = Integer()  # For sample, always None if it exists.
    report_reasons = Keyword()  # For sample, always None if it exists.
    user_reports = Nested()  # For sample, always [] if it exists.
    steward_reports = Nested()  # For sample, always [] if it exists.

    can_mod_post = Boolean()
    no_follow = Boolean()
    removal_reason = Keyword()
    send_replies = Boolean()

    @classmethod
    @overrides
    def prepare_doc_dict(cls, doc_dict: MutableMapping[str, object]) -> None:
        super().prepare_doc_dict(doc_dict)

        # "media_metadata" contains a mapping of arbitrary IDs to some objects. Such a
        # mapping would result in each ID being added as its own field in Elasticsearch.
        # Therefore we convert this to a list of the same objects. The objects already
        # contain a field with the respective ID.
        if doc_dict.get("media_metadata"):
            doc_dict["media_metadata"] = list(
                cast(Mapping[str, object], doc_dict["media_metadata"]).values()
            )


class RedditLink(RedditBaseDocument):
    domain = Keyword()
    url = Keyword()

    title = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    selftext = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    selftext_html = Keyword(doc_values=False, index=False)

    link_flair_background_color = Keyword(doc_values=False, index=False)
    link_flair_css_class = Keyword()
    link_flair_richtext = Nested(RedditFlairRichtext)
    link_flair_template_id = Keyword()
    link_flair_text = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    link_flair_text_color = Keyword(doc_values=False, index=False)
    link_flair_type = Keyword()

    media = Object(RedditLinkMedia)
    media_embed = Object(RedditLinkMediaEmbed)
    secure_media = Object(RedditLinkMedia)
    secure_media_embed = Object(RedditLinkMediaEmbed)
    preview = Object(RedditLinkPreview)
    thumbnail = Keyword(doc_values=False, index=False)
    thumbnail_width = Short(doc_values=False, index=False)
    thumbnail_height = Short(doc_values=False, index=False)

    collections = Nested(RedditLinkCollection)

    crosspost_parent = Keyword()
    # See documentation on this in RedditPost.from_dict().
    # crosspost_parent_list = Nested(RedditLink) # noqa: E800

    allow_live_comments = Boolean()
    brand_safe = Boolean()
    contest_mode = Boolean()
    disable_comments = Boolean()
    hide_score = Boolean()
    is_blank = Boolean()  # For sample, always False if it exists.
    is_crosspostable = Boolean()
    is_meta = Boolean()  # For sample, always None if it exists.
    is_original_content = Boolean()
    is_reddit_media_domain = Boolean()
    is_robot_indexable = Boolean()
    is_self = Boolean()
    is_video = Boolean()
    media_only = Boolean()
    over_18 = Boolean()
    pinned = Boolean()
    quarantine = Boolean()
    spoiler = Boolean()

    category = Keyword()
    content_categories = Keyword(multi=True)
    discussion_type = Keyword()  # For sample, always None if it exists.
    post_categories = Keyword(multi=True)  # For sample, always None if it exists.
    post_hint = Keyword()
    suggested_sort = Keyword()

    previous_visits = RedditDate(multi=True)
    view_count = Integer()  # For sample, always None if it exists.

    whitelist_status = Keyword()
    wls = Short()
    parent_whitelist_status = Keyword()
    pwls = Short()

    num_comments = Integer()
    num_crossposts = Integer()

    event_is_live = Boolean()
    event_start = RedditDate()
    event_end = RedditDate()

    # Promotion-related.
    call_to_action = Keyword()
    domain_override = Keyword()
    embed_type = Keyword()
    embed_url = Keyword()
    href_url = Keyword()
    mobile_ad_url = Keyword(doc_values=False, index=False)
    outbound_link = Object(RedditLinkOutboundLink)
    promoted = Boolean()
    promoted_by = Long()
    show_media = Boolean()
    third_party_trackers = Keyword(multi=True, doc_values=False, index=False)
    third_party_tracking = Keyword(doc_values=False, index=False)
    third_party_tracking_2 = Keyword(doc_values=False, index=False)

    # Log-in required.
    hidden = Boolean()
    clicked = Boolean()
    visited = Boolean()

    # Moderator required.
    ignore_reports = Boolean()  # For sample, always False if it exists.
    removed = Boolean()  # For sample, always False if it exists.
    spam = Boolean()  # For sample, always False if it exists.

    # No idea what these are.
    # There is a "from" field in the Reddit JSON sometimes. However, from is a keyword
    # in Python and therefore can't be used as an attribute name. I opened an issue on
    # this: https://github.com/elastic/elasticsearch-dsl-py/issues/1345
    # from = Keyword()  # noqa: E800  # For sample, always None if it exists.
    from_id = Keyword()  # For sample, always None if it exists.
    from_kind = Keyword()  # For sample, always None if it exists.

    @classmethod
    @overrides
    def prepare_doc_dict(cls, doc_dict: MutableMapping[str, object]) -> None:
        super().prepare_doc_dict(doc_dict)
        doc_dict["_id"] = "t1_" + checked_cast(str, doc_dict["id"])

        # "crosspost_parent_list" contains the whole JSON dict of the post this post
        # is cross-posting somewhere. For simplicity of the data model we discard this
        # here, at the cost of a single ID-lookup to the index should it be needed
        # later.
        doc_dict.pop("crosspost_parent_list", None)


class RedditComment(RedditBaseDocument):
    link_id = Keyword()
    parent_id = Keyword()
    permalink_url = Keyword(doc_values=False, index=False)

    body = Text(
        index_options=_INDEX_OPTIONS,
        index_phrases=_INDEX_PHRASES,
        term_vector=_INDEX_TERM_VECTOR,
        analyzer="standard",
    )
    body_html = Keyword(doc_values=False, index=False)

    controversiality = Integer()
    is_submitter = Boolean()
    quarantined = Boolean()

    collapsed = Boolean()
    collapsed_reason = Keyword()
    collapsed_because_crowd_control = Boolean()

    replies = Keyword()  # For sample, always '' if it exists.

    @classmethod
    @overrides
    def prepare_doc_dict(cls, doc_dict: MutableMapping[str, object]) -> None:
        super().prepare_doc_dict(doc_dict)
        doc_dict["_id"] = "t3_" + checked_cast(str, doc_dict["id"])


class RedditDocument(RedditLink, RedditComment):
    @classmethod
    @overrides
    def prepare_doc_dict(cls, doc_dict: MutableMapping[str, object]) -> None:
        if "title" in doc_dict and "body" in doc_dict:
            raise ValueError("Given post appears to be both link and comment.")
        elif "title" in doc_dict:
            return RedditLink.prepare_doc_dict(doc_dict)
        elif "body" in doc_dict:
            return RedditComment.prepare_doc_dict(doc_dict)
        raise ValueError("Could not determine whether given post is link or comment.")
