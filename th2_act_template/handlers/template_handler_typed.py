# Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
from typing import List

from google.protobuf.text_format import MessageToString
from th2_act_core import GrpcMethodAttributes, HandlerAttributes, RequestProcessor, ActMessage
from th2_common_utils.converters.message_converters import dict_to_message
from th2_grpc_act_template import act_template_typed_pb2_grpc
from th2_grpc_act_template.act_template_pb2 import PlaceSecurityListResponse
from th2_grpc_act_template.act_template_typed_pb2 import PlaceMessageMultiResponseTyped, PlaceMessageRequestTyped, \
    PlaceMessageResponseTyped, NewOrderSingle, ResponseMessageTyped, ExecutionReport, Quote, QuoteStatusReport, \
    NoPartyIDs
from th2_grpc_common.common_pb2 import RequestStatus, Message

logger = logging.getLogger()


class ActHandler(act_template_typed_pb2_grpc.ActTypedServicer):

    def __init__(self, handler_attrs: HandlerAttributes):
        # handler_attrs initialized automatically when ACt server starts. Don't change __init__ method arguments.
        self.handler_attrs = handler_attrs

        # RequestProcessor will not store Heartbeats in its cache
        self.heartbeat_prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

    def placeOrderFIX(self, request: PlaceMessageRequestTyped, context) -> PlaceMessageResponseTyped:
        logger.debug(f'placeOrderFIX received request: {MessageToString(request.metadata, as_one_line=True)}')

        # Attributes of placeOrderFIX() method, used by RequestProcessor
        grpc_method_attrs = GrpcMethodAttributes(method_name='Place order FIX',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        # Get th2-message from request
        typed_msg: NewOrderSingle = request.message_typed.new_order_single
        request_msg: Message = dict_to_message(parent_event_id=request.parent_event_id,
                                               session_alias=request.metadata.id.connection_id.session_alias,
                                               message_type='NewOrderSingle',
                                               fields={
                                                   'OrdType': typed_msg.ord_type,
                                                   'AccountType': typed_msg.account_type,
                                                   'Country': 'USA',
                                                   'OrderCapacity': typed_msg.order_capacity,
                                                   'OrderQty': typed_msg.order_qty,
                                                   'DisplayQty': typed_msg.display_qty,
                                                   'Price': typed_msg.price,
                                                   'ClOrdID': typed_msg.cl_ord_id,
                                                   'SecondaryClOrdID': typed_msg.secondary_cl_ord_id,
                                                   'Side': typed_msg.side,
                                                   'TimeInForce': typed_msg.time_in_force,
                                                   'TransactTime': typed_msg.transact_time,
                                                   'TradingParty': {
                                                       'NoPartyIDs': [
                                                           {
                                                               'PartyID': no_party_id.party_id,
                                                               'PartyIDSource': no_party_id.party_id_source,
                                                               'PartyRole': no_party_id.party_role
                                                           }
                                                           for no_party_id in typed_msg.trading_party.no_party_ids
                                                       ]
                                                   },
                                                   'Instrument': {
                                                       'Symbol': 'qwerty',
                                                       'SecurityID': typed_msg.security_id,
                                                       'SecurityIDSource': typed_msg.security_id_source,
                                                   }
                                               })

        # Method to form response for script
        create_response_message = lambda message: ResponseMessageTyped()  # TODO: fill in

        # Start RequestProcessor context manager with the alias 'rp'
        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=self.heartbeat_prefilter) as rp:
            # Send th2-message and receive its echo as ActMessage
            request_msg_echo: ActMessage = rp.send(request_msg, echo_key_field='ClOrdID')

            # Describe filter for ExecutionReport message as lambda
            # ClOrdID of response message should be equal to this field in request message
            # response message type should be ExecutionReport
            execution_report_filter = lambda response_msg: \
                response_msg['ClOrdID'] == request_msg['ClOrdID'] \
                and response_msg.metadata.message_type == 'ExecutionReport'

            # Describe filter for BusinessMessageReject message as lambda
            # You can describe as many filters as you want
            business_reject_filter = lambda response_msg: \
                response_msg['BusinessRejectRefID'] == request_msg['ClOrdID'] \
                and response_msg.metadata.message_type == 'BusinessMessageReject'

            # Describe filter for SystemReject message as lambda based on MsgSeqNum field of request echo message
            system_reject_filter = lambda response_msg: \
                response_msg['RefSeqNum'] == request_msg_echo.message['header']['MsgSeqNum']

            # First matching message from cache receiving (within 10s timeout)
            # If ExecutionReport is received, report has status RequestStatus.SUCCESS (pass_on)
            # and RequestStatus.ERROR if BusinessMessageReject or SystemReject are received (fail_on)
            report: ActMessage = rp.receive_first_matching(pass_on=execution_report_filter,
                                                           fail_on=(business_reject_filter, system_reject_filter),
                                                           timeout=10)

            # Status is taken from report and checkpoint is taken from RequestProcessor itself
            # Message is also taken from report, but remember that you need to convert it
            # to ResponseMessageTyped object, if you want to put the message in PlaceMessageResponseTyped response
            return PlaceMessageResponseTyped(response_message=create_response_message(report),
                                             status=RequestStatus(status=report.status),
                                             checkpoint_id=rp.checkpoint)

    def placeQuoteFIX(self, request: PlaceMessageRequestTyped, context) -> PlaceMessageMultiResponseTyped:
        logger.debug(f'placeQuoteFIX received request: {MessageToString(request.metadata, as_one_line=True)}')

        # Attributes of placeQuoteFIX() method, used by RequestProcessor
        grpc_method_attrs = GrpcMethodAttributes(method_name='Place quote FIX',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        # Get th2-message from request
        typed_msg: Quote = request.message_typed.quote
        request_msg: Message = dict_to_message(parent_event_id=request.parent_event_id,
                                               session_alias=request.metadata.id.connection_id.session_alias,
                                               message_type=request.metadata.message_type,
                                               fields={
                                                   'NoQuoteQualifiers': [
                                                       {'QuoteQualifier': no_quote_qualifier.quote_qualifier}
                                                       for no_quote_qualifier in typed_msg.no_quote_qualifiers
                                                   ],
                                                   'OfferPx': typed_msg.offer_px,
                                                   'OfferSize': typed_msg.offer_size,
                                                   'QuoteID': typed_msg.quote_id,
                                                   'Symbol': typed_msg.symbol,
                                                   'SecurityIDSource': typed_msg.security_id_source,
                                                   'BidSize': typed_msg.bid_size,
                                                   'BidPx': typed_msg.bid_px,
                                                   'SecurityID': typed_msg.security_id,
                                                   'NoPartyIDs': [
                                                       {
                                                           'PartyID': no_party_id.party_id,
                                                           'PartyIDSource': no_party_id.party_id_source,
                                                           'PartyRole': no_party_id.party_role
                                                       }
                                                       for no_party_id in typed_msg.no_party_ids
                                                   ],
                                                   'QuoteType': typed_msg.quote_type,
                                                   'AccountType': typed_msg.account_type
                                               })

        # Method to form response for script
        create_quote_status_report_message = lambda message: \
            ResponseMessageTyped(quote_status_report=QuoteStatusReport()) \
            if message is not None \
            else ResponseMessageTyped()

        # Method to form response of quotes for script
        create_quote_message_list = lambda message_list: [
            PlaceMessageResponseTyped(
                response_message=ResponseMessageTyped(quote=Quote()),
                status=RequestStatus(status=quote.status),
                checkpoint_id=rp.checkpoint
            )
            for quote in quotes
        ]

        # Start RequestProcessor context manager with the alias 'rp'
        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=self.heartbeat_prefilter) as rp:

            # Send th2-message
            rp.send(request_msg)

            # Describe filters for QuoteStatusReport messages as lambdas
            quote_report_accepted_filter = lambda response_msg: \
                response_msg['QuoteID'] == request_msg['QuoteID'] \
                and response_msg.metadata.message_type == 'QuoteStatusReport' \
                and response_msg['QuoteStatus'] == '0'

            quote_report_rejected_filter = lambda response_msg: \
                response_msg['QuoteID'] == request_msg['QuoteID'] \
                and response_msg.metadata.message_type == 'QuoteStatusReport' \
                and response_msg['QuoteStatus'] == '5'

            # First matching message from cache receiving (within 10s timeout)
            # If QuoteStatusReport with QuoteStatus == 0 is received, report has status RequestStatus.SUCCESS (pass_on)
            # and RequestStatus.ERROR if QuoteStatus == 5 (fail_on)
            quote_status_report: ActMessage = rp.receive_first_matching(
                pass_on=quote_report_accepted_filter,
                fail_on=quote_report_rejected_filter,
                timeout=10
            )

            place_message_response_report: PlaceMessageResponseTyped = PlaceMessageResponseTyped(
                response_message=create_quote_status_report_message(quote_status_report.message),
                status=RequestStatus(status=quote_status_report.status),
                checkpoint_id=rp.checkpoint
            )

            # If QuoteStatusReport with ERROR status was received, stop placeQuoteFIX execution and
            # return response to script
            if place_message_response_report.status.status == RequestStatus.ERROR:
                return PlaceMessageMultiResponseTyped(place_message_response_typed=[place_message_response_report])

            # Otherwise, we continue test scenario
            else:
                # Describe filter for Quote messages as lambda
                quote_filter = lambda response_msg: \
                    response_msg['Symbol'] == request_msg['Symbol'] \
                    and response_msg['NoQuoteQualifiers'][0]['QuoteQualifier'] == 'R' \
                    and response_msg['QuoteType'] == '0'

                # All matching messages from cache receiving. Wait 5s before checking cache in order to receive
                # all quotes. If you expect exact number of messages, use receive_first_n_matching() method
                quotes: List[ActMessage] = rp.receive_all_matching(pass_on=quote_filter,
                                                                   wait_time=5)

                # Form response of quotes for script, using custom converter method
                place_message_response_quotes: List[PlaceMessageResponseTyped] = create_quote_message_list(quotes)
                # Return response with QuoteStatusReport and quotes
                return PlaceMessageMultiResponseTyped(
                    place_message_response_typed=[place_message_response_report, *place_message_response_quotes])

    def placeSecurityListRequest(self, request: PlaceMessageRequestTyped, context) -> PlaceSecurityListResponse:
        logger.debug(f'placeSecurityListRequest received request: '
                     f'{MessageToString(request.metadata, as_one_line=True)}')

        # Attributes of placeSecurityListRequest() method, used by RequestProcessor
        grpc_method_attrs = GrpcMethodAttributes(method_name='Place security list request',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        # Test scenario involves obtaining several SecurityList, the last of which contains
        # the LastFragment == true field. We will prefilter SecurityList messages.
        prefilter = lambda incoming_message: \
            incoming_message.metadata.message_type == 'SecurityList' \
            and incoming_message['SecurityReqID'] == request.message_typed.security_list_request.security_req_id

        # Get th2-message from request, using custom converter method
        typed_msg = request.message_typed.security_list_request
        request_msg: Message = dict_to_message(parent_event_id=request.parent_event_id,
                                               session_alias=request.metadata.id.connection_id.session_alias,
                                               message_type=request.metadata.message_type,
                                               fields={
                                                   'SecurityListRequestType': typed_msg.security_list_request_type,
                                                   'SecurityReqID': typed_msg.security_req_id
                                               })

        # Method to form response for script
        # FIXME: probably should use simple list instead of {index: value} dict
        create_security_list_dict = lambda message_list: {}

        # Start RequestProcessor context manager with the alias 'rp'
        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=prefilter) as rp:
            rp.send(request_msg)  # Send message, no echo will be received

            # Describe filter for SecurityList message with LastFragment == true field as lambda
            last_fragment_filter = lambda response_msg: response_msg['LastFragment'] == 'true'

            # All matching messages before LastFragment message (remember that we prefiltered SecurityList already)
            security_list: List[ActMessage] = rp.receive_all_before_matching(pass_on=last_fragment_filter,
                                                                             timeout=20)

            return PlaceSecurityListResponse(
                securityListDictionary=create_security_list_dict(security_list),
                status=RequestStatus(status=security_list[0].status),
                checkpoint_id=rp.checkpoint
            )
