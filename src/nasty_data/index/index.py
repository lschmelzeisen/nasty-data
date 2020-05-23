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
from logging import Logger, getLogger
from typing import Iterator, Mapping, MutableMapping, Type, TypeVar, cast

from elasticsearch.helpers import bulk
from elasticsearch_dsl import Document, Index, connections
from typing_extensions import Final

_LOGGER: Final[Logger] = getLogger(__name__)


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
        return cls(
            cls.prepare_doc_dict(cast(MutableMapping[str, object], deepcopy(doc_dict)))
        )

    @classmethod
    def prepare_doc_dict(
        cls, doc_dict: MutableMapping[str, object]
    ) -> MutableMapping[str, object]:
        return doc_dict


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

    _LOGGER.debug(f"Creating new index {index_base_name}.")

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


def add_dicts_to_index(
    index_name: str,
    document_cls: Type[_T_BaseDocument],
    dicts: Iterator[Mapping[str, object]],
) -> None:
    def make_elasticsearch_document_dicts() -> Iterator[Mapping[str, object]]:
        for dict_ in dicts:
            document = document_cls.from_dict(dict_)

            # Deserialize data and then serialize again. Needed so that our Python
            # conversion of some data types arrives in the JSON send to ElasticSearch.
            document.full_clean()

            document_dict = dict(document.to_dict(include_meta=True))
            document_dict["_index"] = index_name
            yield document_dict

    _LOGGER.debug(f"Indexing documents of type {document_cls} to index '{index_name}'.")

    _num_success, num_failed = bulk(
        connections.get_connection(),
        make_elasticsearch_document_dicts(),
        stats_only=True,
        raise_on_error=True,
        raise_on_exception=True,
        max_retries=5,
    )

    if num_failed:
        _LOGGER.error("Indexing failed.")
    else:
        _LOGGER.debug("Indexing successful.")


def analyze_index(index_name: str, document_cls: Type[_T_BaseDocument]) -> None:
    ensure_index_exists(index_name)
    _log_mapping_diff(index_name, document_cls)


def _log_mapping_diff(index_name: str, document_cls: Type[_T_BaseDocument]) -> None:
    _LOGGER.debug(
        f"Logging mapping difference between current mapping of index '{index_name}' "
        f"and mapping induced by document class {document_cls}."
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
            _LOGGER.info(indent + field + ": only exists in current dynamic mapping.")
            _LOGGER.info(f"{indent}  [current]")
            _log_field_mapping(current_field_mapping, depth=depth + 1)
            continue

        _LOGGER.info(indent + field + ":")
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
            _LOGGER.info(f"{indent}  [current]")
            _log_field_mapping(current_field_mapping, depth=depth + 1)
            _LOGGER.info(f"{indent}  [induced]")
            _log_field_mapping(induced_field_mapping, depth=depth + 1)

    for field, induced_field_mapping in induced_mapping.items():
        _LOGGER.info(indent + field + ": only exists in induced mapping.")
        _LOGGER.info(f"{indent}  [induced]")
        _log_field_mapping(induced_field_mapping, depth=depth + 1)


def _log_field_mapping(field_mapping: Mapping[str, object], *, depth: int) -> None:
    indent = "  " * depth
    for line in json.dumps(field_mapping, indent=2, sort_keys=False).splitlines():
        if line == "{" or line == "}":
            continue
        _LOGGER.info(indent + line)
