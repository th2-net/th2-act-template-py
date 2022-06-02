# Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

import logging
from typing import Dict, List

from th2_act.act_response import ActMultiResponse
from th2_grpc_act_template.act_template_pb2 import Symbols

logger = logging.getLogger()


def create_security_list_dictionary(act_multi_response: ActMultiResponse) -> Dict[int, Symbols]:
    """Creates dict
    {1: [symbol1, ... , symbol100],
     2: [symbol101, ... , symbol200],
     ...}
     """

    symbols_list = []
    for response in act_multi_response.messages:
        for no_related_sym in response['NoRelatedSym']:
            symbols_list.append(no_related_sym['Symbol'])

    split_symbols_list = [Symbols(symbol=symbols_list[pos:pos + 100]) for pos in range(0, len(symbols_list), 100)]

    security_list_dict = {i: symbols for i, symbols in enumerate(split_symbols_list)}

    return security_list_dict
