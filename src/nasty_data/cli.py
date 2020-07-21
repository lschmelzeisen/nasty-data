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

from datetime import date
from inspect import signature
from logging import getLogger
from pathlib import Path
from typing import Callable, Iterator, Mapping, Optional, Type, TypeVar, cast

from nasty_utils import (
    Argument,
    ArgumentGroup,
    ColoredBraceStyleAdapter,
    Program,
    ProgramConfig,
    SettingsConfig,
    lookup_qualified_name,
    parse_yyyy_mm,
    safe_issubclass,
)
from overrides import overrides
from pydantic import validator

import nasty_data
from nasty_data.elasticsearch_.index import (
    BaseDocument,
    add_documents_to_index,
    analyze_index,
    new_index,
)
from nasty_data.elasticsearch_.settings import ElasticsearchSettings
from nasty_data.source.pushshift import (
    PushshiftDumpType,
    download_pushshift_dumps,
    sample_pushshift_dumps,
)

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_T_BaseDocument = TypeVar("_T_BaseDocument", bound=BaseDocument)
_T_Validator = classmethod


class _NastyElasticsearchSettings(ElasticsearchSettings):
    class Config(SettingsConfig):
        search_path = Path("nasty.toml")


def _document_cls_validator(value: str) -> Type[BaseDocument]:
    document_cls = lookup_qualified_name(value)
    if not safe_issubclass(document_cls, BaseDocument):
        raise ValueError(
            f"Given value {repr(document_cls)} is not a subclass of BaseDocument."
        )
    return cast(Type[BaseDocument], document_cls)


def _load_document_dicts_func_validator(
    value: str,
) -> Callable[[Path], Iterator[Mapping[str, object]]]:
    load_document_dicts_func = lookup_qualified_name(value)
    if not callable(load_document_dicts_func):
        raise ValueError(
            f"Given value {repr(load_document_dicts_func)} is not a callable."
        )

    sig = signature(load_document_dicts_func)
    if sig.return_annotation == sig.empty:
        _LOGGER.warning(
            "Can not verify the return type of given function {!r} because no type "
            "annotation exists.",
            load_document_dicts_func,
        )
    elif sig.return_annotation != Iterator[Mapping[str, object]]:
        raise ValueError(
            f"Return type annotation of given function "
            f"{repr(load_document_dicts_func)} is not "
            f"{repr(Iterator[Mapping[str, object]])} but instead "
            f"{repr(sig.return_annotation)}."
        )

    if len(sig.parameters) < 1:
        raise ValueError(
            f"Given function {repr(load_document_dicts_func)} does not accept a "
            f"parameter."
        )

    param = next(iter(sig.parameters.values()))
    if param.annotation == sig.empty:
        _LOGGER.warning(
            "Can not verify the parameter type of given function {!r} because the "
            "parameter has no type annotation.",
            load_document_dicts_func,
        )
    elif param.annotation != Path:
        raise ValueError(
            f"Parameter type annotation of given function "
            f"{repr(load_document_dicts_func)} is not {repr(Path)} but instead "
            f"`{repr(param.annotation)}."
        )

    return cast(
        Callable[[Path], Iterator[Mapping[str, object]]], load_document_dicts_func
    )


def _yyyy_mm_validator(value: Optional[str]) -> Optional[date]:
    return parse_yyyy_mm(value) if value else None


_NEW_INDEX_ARGUMENT_GROUP = ArgumentGroup(name="New Index Arguments")


class _NewIndexProgram(Program):
    class Config(ProgramConfig):
        title = "new-index"
        aliases = ("n",)
        description = (
            "Create new Elasticsearch index with current settings and mappings for "
            "given dump type, update index alias."
        )

    settings: _NastyElasticsearchSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    index_name: str = Argument(
        alias="name",
        short_alias="n",
        description="Base name of the index (timestamp will be appended).",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )
    document_cls: Type[BaseDocument] = Argument(
        alias="doc-cls",
        short_alias="d",
        description="Fully-qualified class name of BaseDocument subclass.",
        metavar="FQN",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )
    move_data: bool = Argument(
        False,
        alias="move-data",
        description="Reindex data from previous index into new one.",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )
    update_alias: bool = Argument(
        True,
        alias="update-alias",
        description="Update alias of index base name to point to new index.",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )

    _document_cls_validator: _T_Validator = validator(
        "document_cls", pre=True, allow_reuse=True
    )(_document_cls_validator)

    @overrides
    def run(self) -> None:
        self.settings.setup_elasticsearch_connection()
        new_index(
            self.index_name,
            self.document_cls,
            move_data=self.move_data,
            update_alias=self.update_alias,
        )


_INDEX_DUMP_ARGUMENT_GROUP = ArgumentGroup(name="Index Dump Arguments")


class _IndexDumpProgram(Program):
    class Config(ProgramConfig):
        title = "index-dump"
        aliases = ("i",)
        description = "Add contents of a given post dump to Elasticsearch index."

    settings: _NastyElasticsearchSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    index_name: str = Argument(
        alias="name",
        short_alias="n",
        description="Name of the index.",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )
    document_cls: Type[BaseDocument] = Argument(
        alias="doc-cls",
        short_alias="d",
        description="Fully-qualified class name of BaseDocument subclass.",
        metavar="FQN",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )
    load_document_dicts_func: Callable[
        [Path], Iterator[Mapping[str, object]]
    ] = Argument(
        alias="load-fun",
        short_alias="l",
        description=(
            "Fully-qualified name of function taking a file path and yielding document "
            "dicts (passed to --class init)."
        ),
        metavar="FQN",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )
    file: Path = Argument(
        short_alias="f",
        description="Dump file containing all posts to index.",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )
    num_procs: int = Argument(
        0,
        alias="num-procs",
        description=(
            "Number of processors to use for parallel preprocessing "
            "(default: 0, detects number of available processors)."
        ),
        metavar="N",
        group=_NEW_INDEX_ARGUMENT_GROUP,
    )

    _document_cls_validator: _T_Validator = validator(
        "document_cls", pre=True, allow_reuse=True
    )(_document_cls_validator)
    _load_document_dicts_func_validator: _T_Validator = validator(
        "load_document_dicts_func", pre=True, allow_reuse=True
    )(_load_document_dicts_func_validator)

    @overrides
    def run(self) -> None:
        self.settings.setup_elasticsearch_connection()
        add_documents_to_index(
            self.index_name,
            self.document_cls,
            # Need type: ignore because of https://github.com/python/mypy/issues/708
            self.load_document_dicts_func(self.file),  # type: ignore
            max_retries=self.settings.elasticsearch.max_retries,
            num_procs=self.num_procs if self.num_procs > 0 else None,
        )


_ANALYZE_INDEX_ARGUMENT_GROUP = ArgumentGroup(name="Analyze Index Arguments")


class _AnalyzeIndexProgram(Program):
    class Config(ProgramConfig):
        title = "analyze-index"
        aliases = ("a",)
        description = "Analyze mappings of an index."

    settings: _NastyElasticsearchSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    index_name: str = Argument(
        alias="name",
        short_alias="n",
        description="Name of the index.",
        group=_ANALYZE_INDEX_ARGUMENT_GROUP,
    )
    document_cls: Type[BaseDocument] = Argument(
        alias="doc-cls",
        short_alias="d",
        description="Fully-qualified class name of BaseDocument subclass.",
        metavar="FQN",
        group=_ANALYZE_INDEX_ARGUMENT_GROUP,
    )

    _document_cls_validator: _T_Validator = validator(
        "document_cls", pre=True, allow_reuse=True
    )(_document_cls_validator)

    @overrides
    def run(self) -> None:
        self.settings.setup_elasticsearch_connection()
        analyze_index(self.index_name, self.document_cls)


_DOWNLOAD_PUSHSHIFT_ARGUMENT_GROUP = ArgumentGroup(name="Download Arguments")


class _DownloadPushshiftProgram(Program):
    class Config(ProgramConfig):
        title = "download"
        aliases = ("dl",)
        description = "Download Pushshift Reddit dumps."

    settings: _NastyElasticsearchSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    directory: Path = Argument(
        alias="dir",
        short_alias="d",
        description="Directory to download dumps to.",
        group=_DOWNLOAD_PUSHSHIFT_ARGUMENT_GROUP,
    )
    dump_type: Optional[PushshiftDumpType] = Argument(
        None,
        alias="type",
        short_alias="t",
        description=(
            "Only load dumps of this type "
            f"({', '.join(t.value for t in PushshiftDumpType)})."
        ),
        group=_DOWNLOAD_PUSHSHIFT_ARGUMENT_GROUP,
    )
    since: Optional[date] = Argument(
        None,
        short_alias="s",
        description=(
            "Month of earliest dump to download in YYYY-MM format (inclusive, "
            "defaults to earliest available)."
        ),
        metavar="DATE",
        group=_DOWNLOAD_PUSHSHIFT_ARGUMENT_GROUP,
    )
    until: Optional[date] = Argument(
        None,
        short_alias="u",
        description=(
            "Month of latest dump to download in YYYY-MM format (inclusive, "
            "defaults to latest available)."
        ),
        metavar="DATE",
        group=_DOWNLOAD_PUSHSHIFT_ARGUMENT_GROUP,
    )

    _since_validator: _T_Validator = validator("since", pre=True, allow_reuse=True)(
        _yyyy_mm_validator
    )
    _until_validator: _T_Validator = validator("until", pre=True, allow_reuse=True)(
        _yyyy_mm_validator
    )

    @overrides
    def run(self) -> None:
        download_pushshift_dumps(
            self.directory,
            dump_type=self.dump_type,
            since=self.since,
            until=self.until,
        )


_SAMPLE_PUSHSHIFT_ARGUMENT_GROUP = ArgumentGroup(name="Sample Arguments")


class _SamplePushshiftProgram(Program):
    class Config(ProgramConfig):
        title = "sample"
        aliases = ("s",)
        description = "Produce a sample of all downloaded Pushshift dumps."

    settings: _NastyElasticsearchSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )

    directory: Path = Argument(
        alias="dir",
        short_alias="d",
        description="Directory containing dumps. Samples will be written here.",
        group=_SAMPLE_PUSHSHIFT_ARGUMENT_GROUP,
    )

    @overrides
    def run(self) -> None:
        sample_pushshift_dumps(self.directory)


class _PushshiftProgram(Program):
    class Config(ProgramConfig):
        title = "pushshift"
        aliases = ("pu",)
        description = "Download or sample the Pushshift Reddit dump."
        subprograms = (_DownloadPushshiftProgram, _SamplePushshiftProgram)

    settings: _NastyElasticsearchSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )


class NastyDataProgram(Program):
    class Config(ProgramConfig):
        title = "nasty-data"
        version = nasty_data.__version__
        description = "TODO"
        subprograms = (
            _NewIndexProgram,
            _IndexDumpProgram,
            _AnalyzeIndexProgram,
            _PushshiftProgram,
        )

    settings: _NastyElasticsearchSettings = Argument(
        alias="config", description="Overwrite default config file path."
    )
