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

from logging import Logger, getLogger
from pathlib import Path
from typing import Mapping, cast

from elasticsearch import Elasticsearch
from elasticsearch_dsl import connections
from typing_extensions import Final

LOGGER: Final[Logger] = getLogger(__name__)


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

    LOGGER.debug("Registering Elasticsearch connection.")
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
