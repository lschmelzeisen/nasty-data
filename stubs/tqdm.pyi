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

from __future__ import annotations

from typing import ContextManager, Iterable, Iterator, Optional, TextIO, TypeVar, Union

_T = TypeVar("_T")

class tqdm(Iterable[_T], ContextManager[tqdm[None]]):  # noqa: N801
    # Using T_=None for ContextManager so that tqdm() can be called without specifying a
    # type. Not sure exactly why that works.
    def __init__(
        self,
        iterable: Iterator[_T] = ...,
        desc: str = ...,
        total: Union[int, float] = ...,
        unit: str = ...,
        unit_scale: Union[bool, int, float] = ...,
        unit_divisor: float = ...,
    ): ...
    def __iter__(self) -> Iterator[_T]: ...
    def update(self, n: Union[int, float] = ...) -> None: ...
    @classmethod
    def write(
        cls, s: str, file: Optional[TextIO] = ..., end: str = ..., nolock: bool = ...
    ) -> None: ...
    def close(self) -> None: ...
    n: Union[int, float] = ...
