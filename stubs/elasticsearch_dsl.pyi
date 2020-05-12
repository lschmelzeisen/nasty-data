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

from datetime import date, datetime
from typing import Generic, Mapping, Optional, Sequence, Tuple, Type, TypeVar, Union

from elasticsearch import Elasticsearch

_T_JsonMap = Mapping[str, object]
_T_Using = Union[None, str, Elasticsearch]

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
    ) -> Elasticsearch: ...
    @classmethod
    def get_connection(cls, alias: str = ...) -> Elasticsearch: ...

class char_filter:  # noqa: N801
    def __init__(self, name: str): ...

class tokenizer:  # noqa: N801
    def __init__(self, name: str): ...

class token_filter:  # noqa: N801
    def __init__(
        self,
        name: str,
        type: str = ...,  # noqa: A002
        language: str = ...,
        stopwords: str = ...,
    ): ...

class analyzer:  # noqa: N801
    def __init__(
        self,
        name: str,
        *,
        char_filter: Sequence[Union[str, char_filter]] = ...,
        tokenizer: Union[str, tokenizer] = ...,
        filter: Sequence[Union[str, token_filter]] = ...,  # noqa: A002
    ): ...

class IndexTemplate:
    def save(self, using: _T_Using = ...) -> _T_JsonMap: ...

class Index:
    _name: str
    def __init__(self, name: str, using: _T_Using = ...): ...
    def delete(self, using: _T_Using = ...) -> _T_JsonMap: ...
    def delete_alias(self, name: str, using: _T_Using = ...) -> _T_JsonMap: ...
    def exists(self, using: _T_Using = ...) -> bool: ...
    def exists_alias(self, name: str, using: _T_Using = ...) -> bool: ...
    def get_mapping(self, using: _T_Using = ...) -> _T_JsonMap: ...
    def put_alias(self, name: str, using: _T_Using = ...) -> _T_JsonMap: ...
    def refresh(self, using: _T_Using = ...) -> _T_JsonMap: ...
    def as_template(
        self, template_name: str, pattern: str = ..., order: int = ...
    ) -> IndexTemplate: ...

_T_Index = Union[None, str, Index]
_T_Document = TypeVar("_T_Document", bound="Document")

class InnerDoc: ...

class Document:
    _index: Index
    def __init__(self, *args: object, **kwargs: object): ...
    @classmethod
    def init(cls, index: Union[str, Index], using: _T_Using = ...) -> None: ...
    @classmethod
    def _matches(cls, hit: _T_JsonMap) -> bool: ...
    @classmethod
    def from_es(cls: Type[_T_Document], hit: _T_JsonMap) -> _T_Document: ...
    @classmethod
    def search(
        cls: Type[_T_Document], using: _T_Using = ..., index: _T_Index = ...,
    ) -> Search[_T_Document]: ...
    def full_clean(self) -> None: ...
    def save(
        self,
        validate: bool = ...,
        skip_empty: bool = ...,
        using: _T_Using = ...,
        index: _T_Index = ...,
    ) -> str: ...
    def to_dict(
        self, include_meta: bool = ..., skip_empty: bool = ...
    ) -> _T_JsonMap: ...

class Field:
    def __init__(
        self,
        *,
        multi: bool = ...,
        required: bool = ...,
        doc_values: bool = ...,
        index: bool = ...,
    ): ...

class Boolean(Field): ...
class Short(Field): ...
class Integer(Field): ...
class Long(Field): ...
class Float(Field): ...
class Keyword(Field): ...

class Date(Field):
    def _deserialize(self, data: object) -> Union[datetime, date]: ...

class Text(Field):
    def __init__(
        self,
        *,
        multi: bool = ...,
        required: bool = ...,
        index_options: Optional[str] = ...,
        index_phrases: bool = ...,
        analyzer: Union[None, str, analyzer] = ...,
        fields: Optional[Mapping[str, Text]] = ...,
        term_vector: Optional[str] = ...,
    ): ...

_T_InnerDoc = TypeVar("_T_InnerDoc", bound=InnerDoc)

class Object(Field):
    def __init__(
        self,
        doc_class: Optional[Type[_T_InnerDoc]] = ...,
        dynamic: Union[None, bool, str] = ...,
        properties: Optional[Mapping[str, object]] = ...,
        *,
        multi: bool = ...,
        required: bool = ...,
    ): ...

class Nested(Object): ...

class MetaField:
    def __init__(self, *args: object, **kwargs: object): ...

class Search(Generic[_T_Document]):
    def filter(self, *args: object, **kwargs: object) -> Search[_T_Document]: ...
    def query(self, *args: object, **kwargs: object) -> Search[_T_Document]: ...
    def execute(self) -> Sequence[_T_Document]: ...
