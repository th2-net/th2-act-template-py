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

from typing import List, Dict, Any

from th2_common_utils import dict_to_message
from th2_grpc_act_template.act_template_typed_pb2 import PlaceMessageRequestTyped, Quote, NoPartyIDs
from th2_grpc_common.common_pb2 import Message


# Typed request to non-typed message convertors


def create_message(request_typed: PlaceMessageRequestTyped) -> Message:
    # Unimplemented
    pass


def create_new_order_single(request_typed: PlaceMessageRequestTyped) -> Message:
    message_typed = request_typed.message_typed.new_order_single

    return dict_to_message(parent_event_id=request_typed.parent_event_id,
                           session_alias=request_typed.metadata.id.connection_id.session_alias,
                           message_type=request_typed.metadata.message_type,
                           fields={
                               'OrdType': message_typed.ord_type,
                               'AccountType': message_typed.account_type,
                               'Country': 'USA',
                               'OrderCapacity': message_typed.order_capacity,
                               'OrderQty': message_typed.order_qty,
                               'DisplayQty': message_typed.display_qty,
                               'Price': message_typed.price,
                               'ClOrdID': message_typed.cl_ord_id,
                               'SecondaryClOrdID': message_typed.secondary_cl_ord_id,
                               'Side': message_typed.side,
                               'TimeInForce': message_typed.time_in_force,
                               'TransactTime': message_typed.transact_time,
                               'TradingParty': {
                                   'NoPartyIDs': _create_no_party_ids_fields(message_typed.trading_party.no_party_ids)
                               },
                               'Instrument': {
                                   'Symbol': 'qwerty',
                                   'SecurityID': message_typed.security_id,
                                   'SecurityIDSource': message_typed.security_id_source,
                               }
                           })


def create_quote(request_typed: PlaceMessageRequestTyped) -> Message:
    message_typed = request_typed.message_typed.quote

    return dict_to_message(parent_event_id=request_typed.parent_event_id,
                           session_alias=request_typed.metadata.id.connection_id.session_alias,
                           message_type=request_typed.metadata.message_type,
                           fields={
                               'NoQuoteQualifiers': _create_non_typed_no_quote_qualifiers_fields(
                                   message_typed.no_quote_qualifiers
                               ),
                               'OfferPx': message_typed.offer_px,
                               'OfferSize': message_typed.offer_size,
                               'QuoteID': message_typed.quote_id,
                               'Symbol': message_typed.symbol,
                               'SecurityIDSource': message_typed.security_id_source,
                               'BidSize': message_typed.bid_size,
                               'BidPx': message_typed.bid_px,
                               'SecurityID': message_typed.security_id,
                               'NoPartyIDs': _create_no_party_ids_fields(message_typed.no_party_ids),
                               'QuoteType': message_typed.quote_type
                           })


def create_security_list_request(request_typed: PlaceMessageRequestTyped) -> Message:
    message_typed = request_typed.message_typed.security_list_request

    return dict_to_message(parent_event_id=request_typed.parent_event_id,
                           session_alias=request_typed.metadata.id.connection_id.session_alias,
                           message_type=request_typed.metadata.message_type,
                           fields={
                               'SecurityListRequestType': message_typed.security_list_request_type,
                               'SecurityReqID': message_typed.security_req_id
                           })


def _create_no_party_ids_fields(no_party_ids: List[NoPartyIDs]) -> List[Dict[str, Any]]:
    return [
        {
            'PartyID': no_party_id.party_id,
            'PartyIDSource': no_party_id.party_id_source,
            'PartyRole': no_party_id.party_role
        }
        for no_party_id in no_party_ids
    ]


def _create_non_typed_no_quote_qualifiers_fields(no_quote_qualifiers: List[Quote.QuoteQualifier]) \
        -> List[Dict[str, Any]]:
    return [
        {'QuoteQualifier': no_quote_qualifier.quote_qualifier}
        for no_quote_qualifier in no_quote_qualifiers
    ]
