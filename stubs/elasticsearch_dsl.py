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

from typing import Sequence, Tuple, Union


class connections:  # noqa: N801
    @classmethod
    def create_connection(
        cls,
        alias: str = ...,
        hosts: Union[str, Sequence[str]] = ...,
        port: int = ...,
        http_auth: Tuple[str, str] = ...,
        scheme: str = ...,
        use_ssl: bool = ...,
        ssl_show_warn: bool = ...,
        ssl_assert_hostname: str = ...,
        verify_certs: bool = ...,
        ca_certs: str = ...,
        http_compress: bool = ...,
        max_retries: int = ...,
        timeout: int = ...,
    ) -> None:
        ...
