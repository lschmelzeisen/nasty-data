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
from argparse import Namespace as ArgumentNamespace
from logging import getLogger
from typing import Dict, List

import opamin


class Command:
    command: str = "overwrite-me"
    aliases: List[str] = []
    description: str = "Overwrite me!"

    @classmethod
    def config_argparser(cls, argparser: ArgumentParser) -> None:
        raise NotImplementedError()

    def __init__(self, args: ArgumentNamespace, config: Dict):
        self.args = args
        self.config = config
        self.logger = getLogger(opamin.__name__)

    def run(self) -> None:
        raise NotImplementedError()
