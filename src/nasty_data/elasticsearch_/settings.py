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

from logging import getLogger
from pathlib import Path

from elasticsearch_dsl import connections
from nasty_utils import ColoredBraceStyleAdapter, LoggingSettings, Settings
from pydantic import SecretStr

_LOGGER = ColoredBraceStyleAdapter(getLogger(__name__))


class _ElasticsearchSection(Settings):
    host: str = "localhost"
    port: int = 9200
    user: str = "elastic"
    password: SecretStr = SecretStr("")
    ca_crt_path: Path
    timeout: float = 10.0
    retry_on_timeout: bool = True
    max_retries: int = 5
    http_compress: bool = True


class ElasticsearchSettings(LoggingSettings):
    elasticsearch: _ElasticsearchSection

    def setup_elasticsearch_connection(self) -> None:
        _LOGGER.debug("Setting up Elasticsearch connection.")

        if not self.elasticsearch.ca_crt_path.exists():
            raise FileNotFoundError(
                f"CA-Certificate '{self.elasticsearch.ca_crt_path}' could not be found."
                " Configuration without a certificate is not supported at this time."
            )

        connections.create_connection(
            hosts=[{"host": self.elasticsearch.host, "port": self.elasticsearch.port}],
            timeout=self.elasticsearch.timeout,
            retry_on_timeout=self.elasticsearch.retry_on_timeout,
            max_retries=self.elasticsearch.max_retries,
            http_compress=self.elasticsearch.http_compress,
            scheme="https",
            use_ssl=True,
            http_auth=(
                self.elasticsearch.user,
                self.elasticsearch.password.get_secret_value(),
            ),
            verify_certs=True,
            ssl_show_warn=True,
            ca_certs=str(self.elasticsearch.ca_crt_path),
            ssl_assert_hostname=self.elasticsearch.host,
        )
