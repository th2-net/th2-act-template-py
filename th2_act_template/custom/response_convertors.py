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

from th2_act_core import ActMessage
from th2_grpc_act_template.act_template_pb2 import Symbols
from th2_grpc_act_template.act_template_typed_pb2 import MassQuoteAcknowledgement, NoPartyIDs, OrderMassCancelReport, \
    Quote, QuoteStatusReport, ResponseMessageTyped, PlaceMessageResponseTyped
from th2_grpc_common.common_pb2 import Message, RequestStatus, Checkpoint


# Non-typed messages to typed messages convertors

def create_quote_status_report(message: Message) -> ResponseMessageTyped:
    if message is not None:
        return ResponseMessageTyped(quote_status_report=QuoteStatusReport())
    return ResponseMessageTyped()


def quote_status_repost_to_place_message_response(quote_status_report: ActMessage,
                                                  checkpoint: Checkpoint) -> PlaceMessageResponseTyped:
    return PlaceMessageResponseTyped(
        response_message=create_quote_status_report(quote_status_report.message),
        status=RequestStatus(status=quote_status_report.status),
        checkpoint_id=checkpoint
    )


def create_quote(message: Message) -> ResponseMessageTyped:
    if message is not None:
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
    return ResponseMessageTyped()


def quotes_to_place_message_response(quotes: List[ActMessage],
                                     checkpoint: Checkpoint) -> List[PlaceMessageResponseTyped]:
    return [
        PlaceMessageResponseTyped(
            response_message=create_quote(quote.message),
            status=RequestStatus(status=quote.status),
            checkpoint_id=checkpoint
        )
        for quote in quotes
    ]


def _create_no_party_ids(message: Message) -> NoPartyIDs:
    return NoPartyIDs(party_id=message['PartyID'],
                      party_id_source=message['PartyIDSource'],
                      party_role=int(message['PartyRole']))


def create_order_mass_cancel_report(message: Message) -> ResponseMessageTyped:
    if message is not None:
        return ResponseMessageTyped(
            order_mass_cancel_report=OrderMassCancelReport(
                cl_ord_id=message['ClOrdID'],
                text=message['Text']
            )
        )
    return ResponseMessageTyped()


def create_mass_quote_acknowledgement(message: Message) -> ResponseMessageTyped:
    if message is not None:
        return ResponseMessageTyped(
            mass_quote_acknowledgement=MassQuoteAcknowledgement(
                quote_id=message['QuoteID'],
                text=message['Text']
            )
        )
    return ResponseMessageTyped()


def create_security_list_dictionary(messages: List[ActMessage]) -> Dict[int, Symbols]:
    """Creates dict
    {1: [symbol1, ... , symbol100],
     2: [symbol101, ... , symbol200],
     ...}
     """

    if messages is not None:
        symbols_list = []
        for response in messages:
            for no_related_sym in response.message['NoRelatedSym']:
                symbols_list.append(no_related_sym['Symbol'])

        split_symbols_list = [Symbols(symbol=symbols_list[pos:pos + 100]) for pos in range(0, len(symbols_list), 100)]

        return {i: symbols for i, symbols in enumerate(split_symbols_list)}
    return {}
