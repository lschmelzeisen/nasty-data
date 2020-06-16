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

import logging

from nasty_data.cli import NastyDataProgram
from nasty_data.document.reddit import (
    RedditAwarding,
    RedditAwardingResizedIcon,
    RedditBaseDocument,
    RedditComment,
    RedditDate,
    RedditDocument,
    RedditFlairRichtext,
    RedditGildings,
    RedditLink,
    RedditLinkCollection,
    RedditLinkMedia,
    RedditLinkMediaEmbed,
    RedditLinkMediaOEmbed,
    RedditLinkMediaRedditVideo,
    RedditLinkOutboundLink,
    RedditLinkPreview,
    RedditLinkPreviewImage,
    RedditLinkPreviewImageResolution,
    RedditLinkPreviewImageVariant,
    RedditLinkPreviewImageVariants,
    RedditMediaMetadata,
    RedditMediaMetadataS,
)
from nasty_data.document.twitter import (
    TwitterCoordinates,
    TwitterDocument,
    TwitterEntities,
    TwitterEntitiesAdditionalMediaInfo,
    TwitterEntitiesAdditionalMediaInfoCallToAction,
    TwitterEntitiesAdditionalMediaInfoCallToActions,
    TwitterEntitiesIndicesText,
    TwitterEntitiesMedia,
    TwitterEntitiesMediaExtMediaAvailability,
    TwitterEntitiesMediaFeature,
    TwitterEntitiesMediaFeatures,
    TwitterEntitiesMediaOriginalInfo,
    TwitterEntitiesMediaRect,
    TwitterEntitiesMediaSize,
    TwitterEntitiesMediaSizes,
    TwitterEntitiesMediaVideoInfo,
    TwitterEntitiesMediaVideoInfoVariant,
    TwitterEntitiesUrl,
    TwitterEntitiesUserMention,
    TwitterExt,
    TwitterExtensions,
    TwitterExtensionsMediaColor,
    TwitterExtensionsMediaColorPalette,
    TwitterExtensionsMediaColorPaletteRgb,
    TwitterExtensionsMediaStats,
    TwitterJsonAsStr,
    TwitterPlace,
    TwitterQuotedStatusPermalink,
    TwitterScopes,
    TwitterSelfThread,
    TwitterUser,
    TwitterUserEntities,
    TwitterUserExt,
)
from nasty_data.elasticsearch_.config import ElasticsearchConfig
from nasty_data.elasticsearch_.index import (
    BaseDocument,
    add_documents_to_index,
    analyze_index,
    customize_document_cls,
    ensure_index_exists,
    new_index,
)
from nasty_data.source.nasty_batch_results import (
    NastyBatchMeta,
    NastyBatchResultsTwitterDocument,
    NastyRequestMeta,
    load_document_dicts_from_nasty_batch_results,
)
from nasty_data.source.pushshift import (
    PushshiftDumpMeta,
    PushshiftDumpType,
    PushshiftRedditDocument,
    download_pushshift_dumps,
    load_document_dicts_from_pushshift_dump,
    sample_pushshift_dumps,
)

__all__ = [
    "NastyDataProgram",
    "RedditAwarding",
    "RedditAwardingResizedIcon",
    "RedditBaseDocument",
    "RedditComment",
    "RedditDate",
    "RedditDocument",
    "RedditFlairRichtext",
    "RedditGildings",
    "RedditLink",
    "RedditLinkCollection",
    "RedditLinkMedia",
    "RedditLinkMediaEmbed",
    "RedditLinkMediaOEmbed",
    "RedditLinkMediaRedditVideo",
    "RedditLinkOutboundLink",
    "RedditLinkPreview",
    "RedditLinkPreviewImage",
    "RedditLinkPreviewImageResolution",
    "RedditLinkPreviewImageVariant",
    "RedditLinkPreviewImageVariants",
    "RedditMediaMetadata",
    "RedditMediaMetadataS",
    "TwitterCoordinates",
    "TwitterDocument",
    "TwitterEntities",
    "TwitterEntitiesAdditionalMediaInfo",
    "TwitterEntitiesAdditionalMediaInfoCallToAction",
    "TwitterEntitiesAdditionalMediaInfoCallToActions",
    "TwitterEntitiesIndicesText",
    "TwitterEntitiesMedia",
    "TwitterEntitiesMediaExtMediaAvailability",
    "TwitterEntitiesMediaFeature",
    "TwitterEntitiesMediaFeatures",
    "TwitterEntitiesMediaOriginalInfo",
    "TwitterEntitiesMediaRect",
    "TwitterEntitiesMediaSize",
    "TwitterEntitiesMediaSizes",
    "TwitterEntitiesMediaVideoInfo",
    "TwitterEntitiesMediaVideoInfoVariant",
    "TwitterEntitiesUrl",
    "TwitterEntitiesUserMention",
    "TwitterExt",
    "TwitterExtensions",
    "TwitterExtensionsMediaColor",
    "TwitterExtensionsMediaColorPalette",
    "TwitterExtensionsMediaColorPaletteRgb",
    "TwitterExtensionsMediaStats",
    "TwitterJsonAsStr",
    "TwitterPlace",
    "TwitterQuotedStatusPermalink",
    "TwitterScopes",
    "TwitterSelfThread",
    "TwitterUser",
    "TwitterUserEntities",
    "TwitterUserExt",
    "ElasticsearchConfig",
    "BaseDocument",
    "add_documents_to_index",
    "analyze_index",
    "customize_document_cls",
    "ensure_index_exists",
    "new_index",
    "NastyBatchMeta",
    "NastyBatchResultsTwitterDocument",
    "NastyRequestMeta",
    "load_document_dicts_from_nasty_batch_results",
    "PushshiftDumpMeta",
    "PushshiftDumpType",
    "PushshiftRedditDocument",
    "download_pushshift_dumps",
    "load_document_dicts_from_pushshift_dump",
    "sample_pushshift_dumps",
]

__version__ = "dev"
try:
    from nasty_data._version import __version__  # type: ignore
except ImportError:
    pass

__version_info__ = tuple(
    (int(part) if part.isdigit() else part)
    for part in __version__.split(".", maxsplit=4)
)

# Don't show log messages in applications that don't configure logging.
# See https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
logging.getLogger(__name__).addHandler(logging.NullHandler())
