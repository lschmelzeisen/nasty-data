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
from functools import partial
from importlib import import_module
from inspect import signature
from logging import getLogger
from pathlib import Path
from typing import Callable, Iterator, Mapping, Optional, Type, TypeVar, cast

from overrides import overrides

import nasty_data
from nasty_data.elasticsearch_.config import ElasticsearchConfig
from nasty_data.elasticsearch_.index import (
    BaseDocument,
    add_documents_to_index,
    analyze_index,
    new_index,
)
from nasty_data.source.pushshift import (
    PushshiftDumpType,
    download_pushshift_dumps,
    sample_pushshift_dumps,
)
from nasty_utils import (
    Argument,
    ArgumentGroup,
    ColoredBraceStyleAdapter,
    Command,
    CommandMeta,
    Flag,
    Program,
    ProgramMeta,
    parse_enum_arg,
    parse_yyyy_mm_arg,
)

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_T_BaseDocument = TypeVar("_T_BaseDocument", bound=BaseDocument)


def _get_module_member_from_fqn(fqn: str) -> object:
    if "." not in fqn:
        raise ValueError(f"Not a valid fully-qualified name: '{fqn}'")
    module_name, member_name = fqn.rsplit(".", maxsplit=1)
    module = import_module(module_name)
    member = getattr(module, member_name, None)
    if member is None:
        raise ValueError(f"Could not find member '{member_name}' in module '{module}'.")
    return member


def _get_document_cls_from_fqn(fqn: str) -> Type[BaseDocument]:
    document_cls = _get_module_member_from_fqn(fqn)
    if not isinstance(document_cls, type):
        raise ValueError(f"Given value {repr(document_cls)} is not a class.")
    if not issubclass(document_cls, BaseDocument):
        raise ValueError(
            f"Given class {repr(document_cls)} is not a subclass of BaseDocument."
        )
    return document_cls


def _get_load_document_dicts_func_from_fqn(
    fqn: str,
) -> Callable[[Path], Iterator[Mapping[str, object]]]:
    load_document_dicts_func = _get_module_member_from_fqn(fqn)
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


_DOCUMENT_CLS_ARGUMENT = Argument(
    name="doc-cls",
    short_name="d",
    desc="Fully-qualified class name of BaseDocument subclass.",
    metavar="FQN",
    required=True,
    deserializer=_get_document_cls_from_fqn,
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
    document_cls: Type[BaseDocument] = _DOCUMENT_CLS_ARGUMENT
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
            self.document_cls,
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
    document_cls: Type[BaseDocument] = _DOCUMENT_CLS_ARGUMENT
    load_document_dicts_func: Callable[
        [Path], Iterator[Mapping[str, object]]
    ] = Argument(
        name="load-fun",
        short_name="l",
        desc=(
            "Fully-qualified name of function taking a file path and yielding document "
            "dicts (passed to --class init)."
        ),
        metavar="FQN",
        required=True,
        deserializer=_get_load_document_dicts_func_from_fqn,
    )
    file: Path = Argument(
        name="file",
        short_name="f",
        desc="Dump file containing all posts to index.",
        metavar="FILE",
        required=True,
        deserializer=Path,
    )
    num_procs: int = Argument(
        name="num-procs",
        desc=(
            "Number of processors to use for parallel preprocessing "
            "(default: 0, detects number of available processors)."
        ),
        metavar="N",
        default=0,
        deserializer=int,
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
        add_documents_to_index(
            self.index_name,
            self.document_cls,
            # Need type: ignore because of https://github.com/python/mypy/issues/708
            self.load_document_dicts_func(self.file),  # type: ignore
            max_retries=self.config.elasticsearch.max_retries,
            num_procs=self.num_procs if self.num_procs > 0 else None,
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
    document_cls: Type[BaseDocument] = _DOCUMENT_CLS_ARGUMENT

    @classmethod
    @overrides
    def meta(cls) -> CommandMeta:
        return CommandMeta(
            name="analyze-index", aliases=["a"], desc="Analyze mappings of an index.",
        )

    @overrides
    def run(self) -> None:
        self.config.setup_elasticsearch_connection()
        analyze_index(self.index_name, self.document_cls)


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
            config_file="nasty.toml",
            config_dir=".",
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
