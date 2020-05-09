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

from typing import Callable, Iterator, Mapping, Optional, Sequence, Tuple, Union

from . import Elasticsearch

def bulk(
    client: Elasticsearch,
    actions: Iterator[Mapping[str, object]],
    stats_only: bool = ...,
    chunk_size: int = ...,
    max_chunk_bytes: int = ...,
    raise_on_error: bool = ...,
    expand_action_callback: Callable[
        [Union[str, Mapping[str, object]]],
        Tuple[Mapping[str, object], Optional[Mapping[str, object]]],
    ] = ...,
    raise_on_exception: bool = ...,
    max_retries: int = ...,
    initial_backoff: int = ...,
    max_backoff: int = ...,
) -> Tuple[int, Union[int, Sequence[object]]]: ...
