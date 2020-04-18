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

from argparse import ArgumentParser
from typing import Sequence

from overrides import overrides

from .._command import Command


class ConfigureIndexRedditCommand(Command):
    @classmethod
    @overrides
    def command(cls) -> str:
        return "configure-index"

    @classmethod
    @overrides
    def aliases(cls) -> Sequence[str]:
        return ["ci", "conf"]

    @classmethod
    @overrides
    def description(cls) -> str:
        return (
            "Create the Elasticsearch Reddit index if necessary and configure mappings "
            "and other settings."
        )

    @classmethod
    @overrides
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        pass

    @overrides
    def run(self) -> None:
        pass