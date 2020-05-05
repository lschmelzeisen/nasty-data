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

import logging
from logging import getLogger

import elasticsearch
import urllib3
from tqdm import tqdm


# See https://github.com/tqdm/tqdm/issues/313#issuecomment-346819396
class TqdmStream:
    @classmethod
    def write(cls, msg: str) -> None:
        tqdm.write(msg, end="")


def setup_logging(level: int) -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)1.1s [ %(name)-31s ] %(message)s", level=level
    )

    # Logs all HTTP actions and URLs on INFO.
    getLogger(elasticsearch.__name__).setLevel(logging.WARNING)

    getLogger(urllib3.__name__).setLevel(logging.INFO)
