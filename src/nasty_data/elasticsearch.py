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

from elasticsearch_dsl import connections
from nasty_utils import Config, ConfigAttr, ConfigSection, LoggingConfig
from typing_extensions import Final

_LOGGER: Final[Logger] = getLogger(__name__)


class _ElasticsearchSection(Config):
    host: str = ConfigAttr(default="localhost")
    port: int = ConfigAttr(default=9200)
    user: str = ConfigAttr(default="elastic")
    password: str = ConfigAttr(default="", secret=True)
    ca_crt_path: Path = ConfigAttr(required=True, deserializer=Path, serializer=str)


class ElasticsearchConfig(LoggingConfig):
    elasticsearch: _ElasticsearchSection = ConfigSection()

    def setup_elasticsearch_connection(self) -> None:
        _LOGGER.debug("Setting up Elasticsearch connection.")

        if not self.elasticsearch.ca_crt_path.exists():
            raise FileNotFoundError(
                f"CA-Certificate '{self.elasticsearch.ca_crt_path}' could not be found."
                "Configuration without a certificate is not supported at this time."
            )

        connections.create_connection(
            hosts=self.elasticsearch.host,
            port=self.elasticsearch.port,
            http_auth=(self.elasticsearch.user, self.elasticsearch.password),
            # Use SSL
            scheme="https",
            use_ssl=True,
            ssl_show_warn=True,
            ssl_assert_hostname=self.elasticsearch.host,
            verify_certs=True,
            ca_certs=str(self.elasticsearch.ca_crt_path),
            # Enable HTTP compression because we will probably insert a lot of large
            # documents and the documentation says it will help:
            # https://elasticsearch-py.readthedocs.io/en/master/#compression
            http_compress=True,
            # For development, so errors are seen comparatively fast.
            max_retries=2,
            timeout=3,
        )
