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

from typing import Dict, List

from th2_grpc_act_template.act_template_pb2 import Symbols, PlaceSecurityListResponse
from th2_grpc_act_template.act_template_typed_pb2 import \
    MassQuoteAcknowledgement, OrderMassCancelReport, Quote, QuoteStatusReport, ResponseMessageTyped, NoPartyIDs
from th2_grpc_common.common_pb2 import Message


# Non-typed messages to typed messages convertors

def create_quote_status_report(message: Message) -> ResponseMessageTyped:
    if message is not None:
        return ResponseMessageTyped(quote_status_report=QuoteStatusReport())
    return ResponseMessageTyped()


def create_quote(message: Message) -> ResponseMessageTyped:
    return ResponseMessageTyped(
        quote=Quote(
            no_quote_qualifiers=[
                Quote.QuoteQualifier(quote_qualifier=no_quote_qualifiers['QuoteQualifier'])
                for no_quote_qualifiers in message['NoQuoteQualifiers']
            ],
            offer_px=float(message['OfferPx']),
            offer_size=float(message['OfferSize']),
            quote_id=message['QuoteID'],
            symbol=message['Symbol'],
            security_id_source=message['SecurityIDSource'],
            bid_size=message['BidSize'],
            bid_px=float(message['BidPx']),
            security_id=message['SecurityID'],
            no_party_ids=[
                _create_no_party_ids(no_party_id)
                for no_party_id in message['NoPartyIDs']
            ],
            quote_type=int(message['QuoteType'])
        )
    )


def _create_no_party_ids(message: Message):
    return NoPartyIDs(party_id=message['PartyID'],
                      party_id_source=message['PartyIDSource'],
                      party_role=int(message['PartyRole']))


def create_order_mass_cancel_report(message: Message) -> ResponseMessageTyped:
    return OrderMassCancelReport(
        cl_ord_id=message['ClOrdID'],
        text=message['Text']
    )


def create_mass_quote_acknowledgement(message: Message) -> ResponseMessageTyped:
    return MassQuoteAcknowledgement(
        quote_id=message['QuoteID'],
        text=message['Text']
    )


def create_security_list_dictionary(messages: List[Message]) -> Dict[int, Symbols]:
    """Creates dict
    {1: [symbol1, ... , symbol100],
     2: [symbol101, ... , symbol200],
     ...}
     """

    symbols_list = []
    for response in messages:
        for no_related_sym in response['NoRelatedSym']:
            symbols_list.append(no_related_sym['Symbol'])

    split_symbols_list = [Symbols(symbol=symbols_list[pos:pos + 100]) for pos in range(0, len(symbols_list), 100)]

    security_list_dict = {i: symbols for i, symbols in enumerate(split_symbols_list)}

    return security_list_dict


def create_security_list_response(act_multi_response):
    return PlaceSecurityListResponse(
        securityListDictionary=create_security_list_dictionary(act_multi_response.messages),
        status=act_multi_response.status,
        checkpoint_id=act_multi_response.checkpoint
    )
