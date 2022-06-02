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

from th2_act import ActResponse
from th2_grpc_act_template.act_template_typed_pb2 import BusinessMessageReject, ExecutionReport, \
    MassQuoteAcknowledgement, NoPartyIDs, OrderMassCancelReport, PlaceMessageResponseTyped, Quote, QuoteAck, \
    QuoteStatusReport, ResponseMessageTyped, TradingParty
from th2_grpc_common.common_pb2 import Message


# Non-typed messages to typed messages convertors


def create_business_message_reject(message: Message) -> ResponseMessageTyped:
    return ResponseMessageTyped(
        business_message_reject=BusinessMessageReject(
            ref_msg_type=message['RefMsgType'],
            business_reject_reason=int(message['BusinessRejectReason']),
            business_reject_ref_id=message['BusinessRejectRefID'],
            ref_seq_num=int(message['RefSeqNum']),
            text=message['Text']
        )
    )


def create_execution_report(message: Message) -> ResponseMessageTyped:
    return ResponseMessageTyped(execution_report=ExecutionReport(
        security_id=message['SecurityID'],
        security_id_source=message['SecurityIDSource'],
        ord_type=message['OrdType'],
        account_type=int(message['AccountType']),
        order_capacity=message['OrderCapacity'],
        cl_ord_id=message['ClOrdID'],
        order_qty=float(message['OrderQty']),
        leaves_qty=float(message['LeavesQty']),
        side=message['Side'],
        cum_qty=float(message['CumQty']),
        exec_type=message['ExecType'],
        ord_status=message['OrdStatus'],
        trading_party=_create_trading_party(message['TradingParty']),
        exec_id=message['ExecID'],
        price=float(message['Price']),
        order_id=message['OrderID'],
        text=message['Text'],
        time_in_force=message['TimeInForce'],
        transact_time=message['TransactTime']))


def create_quote_status_report(message: Message) -> ResponseMessageTyped:
    return ResponseMessageTyped(quote_status_report=QuoteStatusReport(text=message.fields['Text'].simple_value))


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


def create_quote_ack(message: Message) -> ResponseMessageTyped:
    return ResponseMessageTyped(quote_ack=QuoteAck(
        rfqid=message['RFQID'],
        text=message['Text'])
    )


def create_order_mass_cancel_report(message: Message) -> ResponseMessageTyped:
    return ResponseMessageTyped(order_mass_cancel_report=OrderMassCancelReport(
        cl_ord_id=message['ClOrdID'],
        text=message['Text'])
    )


def create_mass_quote_acknowledgement(message: Message) -> ResponseMessageTyped:
    return ResponseMessageTyped(mass_quote_acknowledgement=MassQuoteAcknowledgement(
        quote_id=message['QuoteID'],
        text=message['Text'])
    )


def _create_trading_party(message: Message) -> TradingParty:
    return TradingParty(
        no_party_ids=[_create_no_party_ids(no_party_id) for no_party_id in message['NoPartyIDs']]
    )


def _create_no_party_ids(message: Message) -> NoPartyIDs:
    return NoPartyIDs(party_id=message['PartyID'],
                      party_id_source=message['PartyIDSource'],
                      party_role=int(message['PartyRole']))


response_message_type_to_function = {
    'BusinessMessageReject': create_business_message_reject,
    'ExecutionReport': create_execution_report,
    'QuoteStatusReport': create_quote_status_report,
    'Quote': create_quote,
    'QuoteAck': create_quote_ack,
    'OrderMassCancelReport': create_order_mass_cancel_report,
    'CreateMassQuoteAcknowledgement': create_mass_quote_acknowledgement
}


def act_response_to_typed_response(act_response: ActResponse) -> PlaceMessageResponseTyped:
    """Creates the PlaceMessageResponseTyped class instance based on the data of the ActResponse class instance."""

    place_message_response_typed = PlaceMessageResponseTyped(
        status=act_response.status,
        checkpoint_id=act_response.checkpoint
    )

    if act_response.message:
        message_type = act_response.message.metadata.message_type
        convert_message_function = response_message_type_to_function[message_type]
        response_message_typed = convert_message_function(act_response.message)

        place_message_response_typed.metadata.CopyFrom(act_response.message.metadata)
        place_message_response_typed.parent_event_id.CopyFrom(act_response.message.parent_event_id)
        place_message_response_typed.response_message_typed.CopyFrom(response_message_typed)

    return place_message_response_typed
