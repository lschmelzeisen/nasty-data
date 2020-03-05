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

from typing import Iterable, Iterator, Optional, TextIO, TypeVar

_T = TypeVar("_T")

class tqdm(Iterable[_T]):  # noqa: N801
    def __init__(self, iterable: Iterator[_T] = ..., desc: str = ...): ...
    def __iter__(self) -> Iterator[_T]: ...
    @classmethod
    def write(
        cls, s: str, file: Optional[TextIO] = ..., end: str = ..., nolock: bool = ...
    ) -> None: ...
