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

import enum
from datetime import date
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Iterator, Mapping, Optional, Type, TypeVar

from nasty_utils import (
    Argument,
    ArgumentGroup,
    Command,
    CommandMeta,
    Flag,
    Program,
    ProgramMeta,
    parse_enum_arg,
    parse_yyyy_mm_arg,
)
from overrides import overrides

import nasty_data
from nasty_data.elasticsearch.config import ElasticsearchConfig
from nasty_data.elasticsearch.index import (
    BaseDocument,
    add_dicts_to_index,
    analyze_index,
    new_index,
)
from nasty_data.source.nasty_batch_results import (
    NastyBatchResultsTwitterDocument,
    load_dict_from_nasty_batch_results,
)
from nasty_data.source.pushshift import (
    PushshiftDumpType,
    PushshiftRedditDocument,
    download_pushshift_dumps,
    load_dicts_from_pushshift_dump,
    sample_pushshift_dumps,
)

_T_BaseDocument = TypeVar("_T_BaseDocument", bound=BaseDocument)


class _IndexType(Enum):
    TWITTER = enum.auto()
    REDDIT = enum.auto()

    def document_cls(self) -> Type[BaseDocument]:
        return {
            _IndexType.TWITTER: NastyBatchResultsTwitterDocument,
            _IndexType.REDDIT: PushshiftRedditDocument,
        }[self]

    def load_dicts(self, file: Path) -> Iterator[Mapping[str, object]]:
        yield from {
            _IndexType.TWITTER: load_dict_from_nasty_batch_results,
            _IndexType.REDDIT: load_dicts_from_pushshift_dump,
        }[self](file)


_INDEX_TYPE_ARGUMENT = Argument(
    name="type",
    short_name="t",
    desc=f"Type of the index to create ({', '.join(t.name for t in _IndexType)}).",
    metavar="TYPE",
    required=True,
    deserializer=partial(
        parse_enum_arg,
        enum_cls=_IndexType,
        ignore_case=True,
        convert_camel_case_for_error=True,
    ),
)


class _NewIndexCommand(Command[ElasticsearchConfig]):
    _new_index_arguments = ArgumentGroup(name="New Index Arguments")
    index_name: str = Argument(
        name="name",
        short_name="n",
        desc="Base name of the index (timestamp will be appended).",
        metavar="NAME",
        required=True,
    )
    index_type: _IndexType = _INDEX_TYPE_ARGUMENT
    move_data: bool = Flag(
        name="move-data", desc="Reindex data from previous index into new one."
    )
    update_alias: bool = Flag(
        name="update-alias",
        desc="Update alias of index base name to point to new index.",
        default=True,
    )

    @classmethod
    @overrides
    def meta(cls) -> CommandMeta:
        return CommandMeta(
            name="new-index",
            aliases=["n"],
            desc=(
                "Create new Elasticsearch index with current settings and mappings "
                "for given dump type, update index alias."
            ),
        )

    @overrides
    def run(self) -> None:
        self.config.setup_elasticsearch_connection()
        new_index(
            self.index_name,
            self.index_type.document_cls(),
            move_data=self.move_data,
            update_alias=self.update_alias,
        )


class _IndexDumpCommand(Command[ElasticsearchConfig]):
    _index_dump_arguments = ArgumentGroup(name="Index Dump Arguments")
    index_name: str = Argument(
        name="name",
        short_name="n",
        desc="Name of the index.",
        metavar="NAME",
        required=True,
    )
    index_type: _IndexType = _INDEX_TYPE_ARGUMENT
    file: Path = Argument(
        name="file",
        short_name="f",
        desc="Dump file containting all posts to index.",
        metavar="FILE",
        required=True,
        deserializer=Path,
    )

    @classmethod
    @overrides
    def meta(cls) -> CommandMeta:
        return CommandMeta(
            name="index-dump",
            aliases=["i"],
            desc="Add contents of a given post dump to Elasticsearch index.",
        )

    @overrides
    def run(self) -> None:
        self.config.setup_elasticsearch_connection()
        add_dicts_to_index(
            self.index_name,
            self.index_type.document_cls(),
            self.index_type.load_dicts(self.file),
        )


class _AnalyzeIndexCommand(Command[ElasticsearchConfig]):
    _analyze_index_arguments = ArgumentGroup(name="Analyze Index Arguments")
    index_name: str = Argument(
        name="name",
        short_name="n",
        desc="Name of the index.",
        metavar="NAME",
        required=True,
    )
    index_type: _IndexType = _INDEX_TYPE_ARGUMENT

    @classmethod
    @overrides
    def meta(cls) -> CommandMeta:
        return CommandMeta(
            name="analyze-index", aliases=["a"], desc="Analyze mappings of an index.",
        )

    @overrides
    def run(self) -> None:
        self.config.setup_elasticsearch_connection()
        analyze_index(self.index_name, self.index_type.document_cls())


class _PushshiftCommand(Command[ElasticsearchConfig]):
    @classmethod
    @overrides
    def meta(cls) -> CommandMeta:
        return CommandMeta(
            name="pushshift",
            aliases=["pu"],
            desc="Download or sample the Pushshift Reddit dump.",
        )


class _DownloadPushshiftCommand(Command[ElasticsearchConfig]):
    _download_arguments = ArgumentGroup(name="Download Arguments")
    directory: Path = Argument(
        name="dir",
        short_name="d",
        desc="Directory to download dumps to.",
        metavar="DIR",
        required=True,
        deserializer=Path,
    )
    dump_type: Optional[PushshiftDumpType] = Argument(
        name="type",
        short_name="t",
        desc=(
            "Only load dumps of this type "
            f"({', '.join(t.name for t in PushshiftDumpType)})."
        ),
        metavar="TYPE",
        deserializer=partial(
            parse_enum_arg,
            enum_cls=PushshiftDumpType,
            ignore_case=True,
            convert_camel_case_for_error=True,
        ),
    )
    since: Optional[date] = Argument(
        name="since",
        short_name="s",
        desc=(
            "Month of earliest dump to download in YYYY-MM format (inclusive, "
            "defaults to earliest available)."
        ),
        metavar="DATE",
        deserializer=parse_yyyy_mm_arg,
    )
    until: Optional[date] = Argument(
        name="until",
        short_name="u",
        desc=(
            "Month of latest dump to download in YYYY-MM format (inclusive, "
            "defaults to latest available)."
        ),
        metavar="DATE",
        deserializer=parse_yyyy_mm_arg,
    )

    @classmethod
    @overrides
    def meta(cls) -> CommandMeta:
        return CommandMeta(
            name="download", aliases=["dl"], desc="Download Pushshift Reddit dumps."
        )

    @overrides
    def run(self) -> None:
        download_pushshift_dumps(
            self.directory,
            dump_type=self.dump_type,
            since=self.since,
            until=self.until,
        )


class _SamplePushshiftCommand(Command[ElasticsearchConfig]):
    _sample_arguments = ArgumentGroup(name="Sample Arguments")
    directory: Path = Argument(
        name="dir",
        short_name="d",
        desc="Directory to download dumps to.",
        metavar="DIR",
        required=True,
        deserializer=Path,
    )

    @classmethod
    @overrides
    def meta(cls) -> CommandMeta:
        return CommandMeta(
            name="sample",
            aliases=["s"],
            desc="Produce a sample of all downloaded Pushshift dumps.",
        )

    @overrides
    def run(self) -> None:
        sample_pushshift_dumps(self.directory)


class NastyDataProgram(Program[ElasticsearchConfig]):
    @classmethod
    @overrides
    def meta(cls) -> ProgramMeta[ElasticsearchConfig]:
        return ProgramMeta(
            name="nasty-data",
            version=nasty_data.__version__,
            desc="TODO",
            config_type=ElasticsearchConfig,
            config_file="data.toml",
            config_dir="nasty",
            command_hierarchy={
                Command: [
                    _NewIndexCommand,
                    _IndexDumpCommand,
                    _AnalyzeIndexCommand,
                    _PushshiftCommand,
                ],
                _PushshiftCommand: [
                    _DownloadPushshiftCommand,
                    _SamplePushshiftCommand,
                ],
            },
        )
