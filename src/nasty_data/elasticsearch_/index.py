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
from copy import deepcopy
from datetime import datetime
from functools import partial
from logging import getLogger
from multiprocessing.pool import Pool
from typing import (
    Callable,
    Dict,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from elasticsearch.exceptions import ElasticsearchException
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Document, Field, Index, InnerDoc, Object, connections

from nasty_utils import ColoredBraceStyleAdapter

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))

_T_BaseDocument = TypeVar("_T_BaseDocument", bound="BaseDocument")


class BaseDocument(Document):
    @classmethod
    def index_settings(cls) -> MutableMapping[str, object]:
        return {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "codec": "best_compression",
        }

    @classmethod
    def from_dict(
        cls: Type[_T_BaseDocument], doc_dict: Mapping[str, object]
    ) -> _T_BaseDocument:
        doc_dict = cast(MutableMapping[str, object], deepcopy(doc_dict))
        cls.prepare_doc_dict(doc_dict)
        return cls(**doc_dict)

    @classmethod
    def prepare_doc_dict(cls, doc_dict: MutableMapping[str, object]) -> None:
        pass

    @classmethod
    def meta_field(cls) -> Optional[Tuple[str, str]]:
        return None


_T_DocumentMeta = Union[Type[Document], Type[InnerDoc]]
_T_Document = TypeVar("_T_Document", bound=Document)
_T_InnerDoc = TypeVar("_T_InnerDoc", bound=InnerDoc)


@overload
def customize_document_cls(
    document_cls: Type[_T_Document],
    field_callback: Callable[
        [_T_DocumentMeta, str, Field, Optional[Type[InnerDoc]]], Mapping[str, Field]
    ],
    *,
    name_prefix: str,
    superclasses: Tuple[Type[object], ...] = ...,
    recursive: bool = ...,
) -> Type[_T_Document]:
    ...


@overload
def customize_document_cls(
    document_cls: Type[_T_InnerDoc],
    field_callback: Callable[
        [_T_DocumentMeta, str, Field, Optional[Type[InnerDoc]]], Mapping[str, Field]
    ],
    *,
    name_prefix: str,
    superclasses: Tuple[Type[object], ...] = ...,
    recursive: bool = ...,
) -> Type[_T_InnerDoc]:
    ...


def customize_document_cls(
    document_cls: _T_DocumentMeta,
    field_callback: Callable[
        [_T_DocumentMeta, str, Field, Optional[Type[InnerDoc]]], Mapping[str, Field]
    ],
    *,
    name_prefix: str,
    superclasses: Tuple[Type[object], ...] = (),
    recursive: bool = True,
) -> _T_DocumentMeta:
    mapping = document_cls._doc_type.mapping
    inner_classes: MutableMapping[Type[InnerDoc], Type[InnerDoc]] = {}

    new_document_cls_dict: Dict[str, object] = {}

    for field_name in mapping:
        field = mapping[field_name]
        inner_class = None

        if isinstance(field, Object):
            inner_class = field._doc_class
            if recursive:
                if inner_class not in inner_classes:
                    inner_classes[inner_class] = customize_document_cls(
                        inner_class,
                        field_callback,
                        name_prefix=name_prefix,
                        recursive=True,
                    )

                if inner_class != inner_classes[inner_class]:
                    inner_class = inner_classes[inner_class]
                    new_document_cls_dict[field_name] = type(field)(inner_class)

        new_document_cls_dict.update(
            field_callback(document_cls, field_name, field, inner_class)
        )

    if not new_document_cls_dict:
        return document_cls

    return cast(
        _T_DocumentMeta,
        type(
            name_prefix + document_cls.__name__,
            superclasses + (document_cls,),
            new_document_cls_dict,
        ),
    )


def new_index(
    index_base_name: str,
    document_cls: Type[_T_BaseDocument],
    *,
    move_data: bool = False,
    update_alias: bool = True,
) -> str:
    """Creates a new Index with mapping settings from given class.

    The index is versioned by including the current timestamp in its name. Through this,
    existing previous indices with potentially incompatible mappings will not be
    affected. An alias is pointed to the newest index.

    Implements the alias migration pattern, based on:
    https://github.com/elastic/elasticsearch-dsl-py/blob/9b1a39dd47e8678bc4885b03b138293e189471d0/examples/alias_migration.py

    :param index_base_name: The index to create a new version of.
    :param document_cls: The elasticsearch-dsl-based class that defines the mapping.
    :param move_data: If true, reindex all data from the previous index to the new one
          (before updating the alias).
    :param update_alias: If true, move the alias to the newly created index.
    """

    _LOGGER.debug("Creating new index '{}'.", index_base_name)

    new_index_name = index_base_name + "-" + datetime.now().strftime("%Y%m%d-%H%M%S")
    new_index = Index(new_index_name)
    new_index.settings(**document_cls.index_settings())
    # The following is equivalent to `new_index.document(document_cls)` except that it
    # does not add `new_index` as a default index to `document_cls`.
    new_index._doc_types.append(document_cls)
    new_index.create()

    if move_data:
        _LOGGER.info("Reindexing data from previous copy to newly created one...")

        # TODO: test if this works and what happens if no previous index exists.
        connections.get_connection().reindex(
            body={
                "source": {"index": index_base_name},
                "dest": {"index": new_index_name},
            },
            request_timeout=3600,
            # TODO: find out if timeout works for large index
            # TODO: check if parameter name is actually `request_timeout` and not
            #  `timeout` as indicated by source.
        )
        new_index.refresh()

    if update_alias:
        all_indices = Index(index_base_name + "-*")
        if all_indices.exists_alias(name=index_base_name):
            all_indices.delete_alias(name=index_base_name)
        new_index.put_alias(name=index_base_name)

    return new_index_name


def ensure_index_exists(index_name: str) -> None:
    if not Index(index_name).exists():
        raise Exception(f"Elasticsearch index '{index_name}' does not exist.")


def _make_upsert_op(
    document_dict: Mapping[str, object],
    *,
    index_name: str,
    document_cls: Type[BaseDocument],
) -> Mapping[str, object]:
    # Deserialize data and then serialize again. Needed so that our Python
    # conversion of some data types arrives in the JSON send to ElasticSearch.
    document = document_cls.from_dict(document_dict)
    document.full_clean()
    document_dict = document.to_dict(include_meta=False)

    meta_field, meta_field_id = document_cls.meta_field() or (None, None)
    meta_field_data = document_dict.get(meta_field) if meta_field else None

    result: MutableMapping[str, object] = {
        "_id": document.meta.id,
        "_index": index_name,
        "_op_type": "update",
    }
    if not (meta_field and meta_field_id and meta_field_data):
        result["doc_as_upsert"] = True
        result["doc"] = document_dict
    else:
        result["upsert"] = document_dict
        result["script"] = {
            "lang": "painless",
            "source": """
                if (ctx._source.{meta_field} == null) {{
                    ctx._source.{meta_field} = params.meta_field;
                }} else if (ctx._source.{meta_field} instanceof List) {{
                    boolean found = false;
                    for (meta_field in ctx._source.{meta_field}) {{
                        if (
                            meta_field.{meta_field_id}
                            == params.meta_field.{meta_field_id}
                        ) {{
                            found = true;
                            break;
                        }}
                    }}
                    if (!found) {{
                        ctx._source.{meta_field}.add(params.meta_field);
                    }}
                }} else {{
                    if (
                        ctx._source.{meta_field}.{meta_field_id}
                        != params.meta_field.{meta_field_id}
                    ) {{
                        ctx._source.{meta_field} = [
                            ctx._source.{meta_field}, params.meta_field
                        ];
                    }}
                }}
            """.format(
                meta_field=meta_field, meta_field_id=meta_field_id
            ),
            "params": {"meta_field": meta_field_data},
        }
    return result


def add_documents_to_index(
    index_name: str,
    document_cls: Type[BaseDocument],
    document_dicts: Iterator[Mapping[str, object]],
    *,
    max_retries: int = 5,
    num_procs: Optional[int] = None,
) -> None:
    ensure_index_exists(index_name)
    _LOGGER.debug("Indexing documents to index '{}'.", index_name)

    def make_upsert_ops() -> Iterator[Mapping[str, object]]:
        with Pool(processes=num_procs) as pool:
            yield from pool.imap_unordered(
                partial(
                    _make_upsert_op, index_name=index_name, document_cls=document_cls
                ),
                document_dicts,
            )

    num_success, num_failed = bulk(
        connections.get_connection(),
        make_upsert_ops(),
        stats_only=True,
        max_retries=max_retries,
    )

    if num_failed:
        raise ElasticsearchException(
            f"Failed to indexed {num_failed} documents ({num_success} succeeded)."
        )

    _LOGGER.debug("Successfully indexed {} documents.", num_success)


def analyze_index(index_name: str, document_cls: Type[_T_BaseDocument]) -> None:
    ensure_index_exists(index_name)
    _log_mapping_diff(index_name, document_cls)


def _log_mapping_diff(index_name: str, document_cls: Type[_T_BaseDocument]) -> None:
    _LOGGER.debug(
        "Logging mapping difference between current mapping of index '{}' and mapping "
        "induced by document class {}.",
        index_name,
        document_cls,
    )

    current_index = Index(index_name)
    induced_index = Index(
        new_index(
            index_name + "-induced", document_cls, move_data=False, update_alias=False
        )
    )

    try:
        current_mapping = cast(
            Mapping[
                str,
                Mapping[str, Mapping[str, MutableMapping[str, Mapping[str, object]]]],
            ],
            current_index.get_mapping(),
        )
        induced_mapping = cast(
            Mapping[
                str,
                Mapping[str, Mapping[str, MutableMapping[str, Mapping[str, object]]]],
            ],
            induced_index.get_mapping(),
        )

        if not current_mapping or not induced_mapping:
            _LOGGER.error("Could not get induced or current mapping.")
            return

        _recursive_mapping_diff(
            next(iter(current_mapping.values()))["mappings"].get("properties", {}),
            next(iter(induced_mapping.values()))["mappings"].get("properties", {}),
        )

    finally:
        induced_index.delete()


def _recursive_mapping_diff(
    current_mapping: MutableMapping[str, Mapping[str, object]],
    induced_mapping: MutableMapping[str, Mapping[str, object]],
    *,
    depth: int = 0,
) -> None:
    indent = "  " * depth
    for field, current_field_mapping in current_mapping.items():
        induced_field_mapping = induced_mapping.pop(field, None)
        if current_field_mapping == induced_field_mapping:
            continue

        if not induced_field_mapping:
            _LOGGER.info(indent + "{}: only exists in current dynamic mapping.", field)
            _LOGGER.info(indent + "  [current]")
            _log_field_mapping(current_field_mapping, depth=depth + 1)
            continue

        _LOGGER.info(indent + "{}:", field)
        if (
            "properties" in current_field_mapping
            and "properties" in induced_field_mapping
        ):
            _recursive_mapping_diff(
                cast(
                    MutableMapping[str, Mapping[str, object]],
                    current_field_mapping["properties"],
                ),
                cast(
                    MutableMapping[str, Mapping[str, object]],
                    induced_field_mapping["properties"],
                ),
                depth=depth + 1,
            )
        else:
            _LOGGER.info(indent + "  [current]")
            _log_field_mapping(current_field_mapping, depth=depth + 1)
            _LOGGER.info(indent + "  [induced]")
            _log_field_mapping(induced_field_mapping, depth=depth + 1)

    for field, induced_field_mapping in induced_mapping.items():
        _LOGGER.info(indent + "{}: only exists in induced mapping.", field)
        _LOGGER.info(indent + "  [induced]")
        _log_field_mapping(induced_field_mapping, depth=depth + 1)


def _log_field_mapping(field_mapping: Mapping[str, object], *, depth: int) -> None:
    indent = "  " * depth
    for line in json.dumps(field_mapping, indent=2, sort_keys=False).splitlines():
        if line == "{" or line == "}":
            continue
        _LOGGER.info(indent + line.replace("{", "{{").replace("}", "}}"))
