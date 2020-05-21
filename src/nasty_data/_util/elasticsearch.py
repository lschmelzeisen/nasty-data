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
from datetime import datetime
from logging import Logger, getLogger
from pathlib import Path
from typing import Dict, Mapping, cast

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Index, connections
from typing_extensions import Final, Type

_LOGGER: Final[Logger] = getLogger(__name__)


def establish_elasticsearch_connection(config: Mapping[str, object]) -> Elasticsearch:
    c = cast(Mapping[str, object], config["elasticsearch-secrets"])  # Shortcut alias.
    host = cast(str, c["host"])
    port = cast(int, c["port"])
    user = cast(str, c["user"])
    password = cast(str, c["password"])
    ca_crt_path = Path(cast(str, c["ca-crt-path"]))

    if not ca_crt_path.exists():
        raise FileNotFoundError(
            f"CA-Certificate '{ca_crt_path}' could not be found "
            "(configured in 'elasticsearch-secrets.ca-crt-path' of 'config.toml')."
        )

    _LOGGER.debug("Registering Elasticsearch connection.")
    return connections.create_connection(
        hosts=host,
        port=port,
        http_auth=(user, password),
        # Use SSL
        scheme="https",
        use_ssl=True,
        ssl_show_warn=True,
        ssl_assert_hostname=host,
        verify_certs=True,
        ca_certs=str(ca_crt_path),
        # Enable HTTP compression because we will probably insert a lot of large
        # documents and the documentation says it will help:
        # https://elasticsearch-py.readthedocs.io/en/master/#compression
        http_compress=True,
        # For development, so errors are seen comparatively fast.
        max_retries=2,
        timeout=3,
    )


def ensure_elasticsearch_index_available(index: Index) -> None:
    if not index.exists():
        raise Exception(f"ElasticSearch index '{index._name}' does not exist.")


def migrate_elasticsearch_index(
    index: Index,
    mapping_cls: Type[Document],
    *,
    move_data: bool = True,
    update_alias: bool = True,
) -> None:
    """Creates a new Index with mapping settings from given class.

    The index is versioned by including the current timestamp in its name. Through this,
    existing previous indices with potentially incompatible mappings will not be
    affected. An alias is pointed to the newest index.

    Implements the alias migration pattern, based on:
    https://github.com/elastic/elasticsearch-dsl-py/blob/9b1a39dd47e8678bc4885b03b138293e189471d0/examples/alias_migration.py

    :param index: The index to create a new version of.
    :param mapping_cls: The elasticsearch-dsl-based class that defines the mapping
    :param move_data: If true, reindex all data from the previous index to the new one
        (before updating the alias).
    :param update_alias: If true, move the alias to the newly created index.
    """

    _LOGGER.info(f"Creating new index {index._name}.")

    new_index = Index(index._name + "-" + datetime.now().strftime("%Y%m%d-%H%M%S"))
    mapping_cls.init(index=new_index._name)

    if move_data:
        _LOGGER.info("Reindexing data from previous copy to newly created one...")

        # TODO: test if this works and what happens if no previous index exists.
        connections.get_connection().reindex(
            body={
                "source": {"index": index._name},
                "dest": {"index": new_index._name},
            },
            request_timeout=3600,
            # TODO: find out if timeout works for large index
            # TODO: check if parameter name is actually `request_timeout` and not
            #  `timeout` as indicated by source.
        )
        new_index.refresh()

    if update_alias:
        all_indices = Index(index._name + "-*")
        if all_indices.exists_alias(name=index._name):
            all_indices.delete_alias(name=index._name)
        new_index.put_alias(name=index._name)


def debug_dynamic_mapping_difference(index: Index, mapping_cls: Type[Document]) -> None:
    """Log diff from current ElasticSearch mapping to mapping_cls-induced one."""

    ensure_elasticsearch_index_available(index)

    unaltered_index = Index("unaltered-" + index._name)
    mapping_cls.init(index=unaltered_index._name)

    try:
        current_mapping = cast(
            Mapping[str, Mapping[str, Mapping[str, Dict[str, Mapping[str, object]]]]],
            index.get_mapping(),
        )
        unaltered_mapping = cast(
            Mapping[str, Mapping[str, Mapping[str, Dict[str, Mapping[str, object]]]]],
            unaltered_index.get_mapping(),
        )

        if not current_mapping or not unaltered_mapping:
            _LOGGER.error("Could not get unaltered or current mapping.")
            return

        _recursive_mapping_diff(
            next(iter(current_mapping.values()))["mappings"].get("properties", {}),
            next(iter(unaltered_mapping.values()))["mappings"].get("properties", {}),
        )

    finally:
        unaltered_index.delete()


def _recursive_mapping_diff(
    current_mapping: Dict[str, Mapping[str, object]],
    unaltered_mapping: Dict[str, Mapping[str, object]],
    *,
    depth: int = 0,
) -> None:
    indent = "  " * depth
    for field, current_field_mapping in current_mapping.items():
        unaltered_field_mapping = unaltered_mapping.pop(field, None)
        if current_field_mapping == unaltered_field_mapping:
            continue

        if not unaltered_field_mapping:
            _LOGGER.info(indent + field + ": only exists in current dynamic mapping.")
            _LOGGER.info(f"{indent}  [current]")
            _log_field_mapping(current_field_mapping, depth=depth + 1)
            continue

        _LOGGER.info(indent + field + ":")
        if (
            "properties" in current_field_mapping
            and "properties" in unaltered_field_mapping
        ):
            _recursive_mapping_diff(
                cast(
                    Dict[str, Mapping[str, object]],
                    current_field_mapping["properties"],
                ),
                cast(
                    Dict[str, Mapping[str, object]],
                    unaltered_field_mapping["properties"],
                ),
                depth=depth + 1,
            )
        else:
            _LOGGER.info(f"{indent}  [current]")
            _log_field_mapping(current_field_mapping, depth=depth + 1)
            _LOGGER.info(f"{indent}  [unaltered]")
            _log_field_mapping(unaltered_field_mapping, depth=depth + 1)

    for field, unaltered_field_mapping in unaltered_mapping.items():
        _LOGGER.info(indent + field + ": only exists in unaltered mapping.")
        _LOGGER.info(f"{indent}  [unaltered]")
        _log_field_mapping(unaltered_field_mapping, depth=depth + 1)


def _log_field_mapping(field_mapping: Mapping[str, object], *, depth: int) -> None:
    indent = "  " * depth
    for line in json.dumps(field_mapping, indent=2).splitlines():
        if line == "{" or line == "}":
            continue
        _LOGGER.info(indent + line)
