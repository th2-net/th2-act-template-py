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
from th2_act_template.custom import request_convertors as req
import th2_act_template.custom.response_convertors as resp
from th2_grpc_act_template import act_template_typed_pb2_grpc
from th2_grpc_act_template.act_template_pb2 import PlaceSecurityListResponse, SendMessageResponse
from th2_grpc_act_template.act_template_typed_pb2 import PlaceMessageMultiResponseTyped, PlaceMessageRequestTyped, \
    PlaceMessageResponseTyped
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

        # Start RequestProcessor context manager with the alias 'rp'
        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=self.heartbeat_prefilter) as rp:
            # Get th2-message from request, using custom converter method
            request_msg: Message = req.create_new_order_single(request)
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

            # Form response for script
            # Status are taken from report and checkpoint is taken from RequestProcessor itself
            # Message is also taken from report, but remember that you need to convert it
            # to ResponseMessageTyped object, if you want to put the message in PlaceMessageResponseTyped response
            return PlaceMessageResponseTyped(response_message=resp.create_execution_report(message=report.message),
                                             status=RequestStatus(status=report.status),
                                             checkpoint_id=rp.checkpoint)

    def sendMessage(self, request: PlaceMessageRequestTyped, context) -> SendMessageResponse:
        logger.debug(f'sendMessage received request: {MessageToString(request.metadata, as_one_line=True)}')

        grpc_method_attrs = GrpcMethodAttributes(method_name='Send message',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        with RequestProcessor(self.handler_attrs, grpc_method_attrs) as rp:
            request_msg: Message = req.create_message(request)
            rp.send(request_msg)

            return SendMessageResponse()

    def placeQuoteRequestFIX(self, request: PlaceMessageRequestTyped, context) -> PlaceMessageResponseTyped:
        logger.debug(f'placeQuoteRequestFIX received request: {MessageToString(request.metadata, as_one_line=True)}')

        grpc_method_attrs = GrpcMethodAttributes(method_name='Place quote request FIX',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=self.heartbeat_prefilter) as rp:
            request_msg: Message = req.create_message(request)
            rp.send(request_msg)

            quote_report_filter = lambda response_msg: \
                response_msg['QuoteReqID'] == request_msg['QuoteReqID'] \
                and response_msg.metadata.message_type == 'QuoteStatusReport'

            report: ActMessage = rp.receive_first_matching(pass_on=quote_report_filter)

            return PlaceMessageResponseTyped(response_message=resp.create_quote_status_report(report.message),
                                             status=RequestStatus(status=report.status),
                                             checkpoint_id=rp.checkpoint)

    def placeQuoteFIX(self, request: PlaceMessageRequestTyped, context) -> PlaceMessageMultiResponseTyped:
        logger.debug(f'placeQuoteFIX received request: {MessageToString(request.metadata, as_one_line=True)}')

        # Attributes of placeQuoteFIX() method, used by RequestProcessor
        grpc_method_attrs = GrpcMethodAttributes(method_name='Place quote FIX',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        # Start RequestProcessor context manager with the alias 'rp'
        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=self.heartbeat_prefilter) as rp:
            # Get th2-message from request, using custom converter method
            request_msg: Message = req.create_quote(request)
            # Send th2-message and receive its echo as ActMessage
            request_msg_echo: ActMessage = rp.send(request_msg, echo_key_field='QuoteID')

            # Describe filters for QuoteStatusReport messages as lambdas
            quote_report_accepted_filter = lambda response_msg: \
                response_msg['QuoteID'] == request_msg['QuoteID'] \
                and response_msg.metadata.message_type == 'QuoteStatusReport' \
                and response_msg['QuoteStatus'] == '0'

            quote_report_rejected_filter = lambda response_msg: \
                response_msg['QuoteID'] == request_msg['QuoteID'] \
                and response_msg.metadata.message_type == 'QuoteStatusReport' \
                and response_msg['QuoteStatus'] == '5'

            # Describe filter for SystemReject message as lambda based on MsgSeqNum field of request echo message
            system_reject_filter = lambda response_msg: \
                response_msg['RefSeqNum'] == request_msg_echo.message['header']['MsgSeqNum']

            # First matching message from cache receiving (within 10s timeout)
            # If QuoteStatusReport with QuoteStatus == 0 is received, report has status RequestStatus.SUCCESS (pass_on)
            # and RequestStatus.ERROR if QuoteStatus == 5 or SystemReject (fail_on)
            quote_status_report: ActMessage = rp.receive_first_matching(
                pass_on=quote_report_accepted_filter,
                fail_on=(quote_report_rejected_filter, system_reject_filter),
                timeout=10
            )

            # Form response for script, using custom converter method
            place_message_response_report: PlaceMessageResponseTyped = \
                resp.quote_status_repost_to_place_message_response(quote_status_report=quote_status_report,
                                                                   checkpoint=rp.checkpoint)

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
                place_message_response_quotes: List[PlaceMessageResponseTyped] = \
                    resp.quotes_to_place_message_response(quotes=quotes, checkpoint=rp.checkpoint)

                # Return response with QuoteStatusReport and quotes
                return PlaceMessageMultiResponseTyped(
                    place_message_response_typed=[place_message_response_report, *place_message_response_quotes])

    def placeOrderMassCancelRequestFIX(self, request: PlaceMessageRequestTyped, context) -> PlaceMessageResponseTyped:
        logger.debug(f'placeOrderMassCancelRequestFIX received request: '
                     f'{MessageToString(request.metadata, as_one_line=True)}')

        grpc_method_attrs = GrpcMethodAttributes(method_name='Place order mass cancel request FIX',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=self.heartbeat_prefilter) as rp:
            request_msg: Message = req.create_message(request)
            rp.send(request_msg)

            order_mass_cancel_report_filter = lambda response_msg: \
                response_msg['ClOrdID'] == request_msg['ClOrdID'] \
                and response_msg.metadata.message_type == 'OrderMassCancelReport'

            report: ActMessage = rp.receive_first_matching(pass_on=order_mass_cancel_report_filter)

            return PlaceMessageResponseTyped(
                response_message=resp.create_order_mass_cancel_report(report.message),
                status=RequestStatus(status=report.status),
                checkpoint_id=rp.checkpoint
            )

    def placeQuoteCancelFIX(self, request: PlaceMessageRequestTyped, context) -> PlaceMessageResponseTyped:
        logger.debug(f'placeQuoteCancelFIX received request: {MessageToString(request.metadata, as_one_line=True)}')

        grpc_method_attrs = GrpcMethodAttributes(method_name='Place quote cancel FIX',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=self.heartbeat_prefilter) as rp:
            request_msg: Message = req.create_message(request)
            rp.send(request_msg)

            mass_quote_ack_filter = lambda response_msg: \
                response_msg['QuoteID'] == request_msg['QuoteMsgID'] \
                and response_msg.metadata.message_type == 'MassQuoteAcknowledgement'

            ack: ActMessage = rp.receive_first_matching(pass_on=mass_quote_ack_filter)

            return PlaceMessageResponseTyped(
                response_message=resp.create_mass_quote_acknowledgement(ack.message),
                status=RequestStatus(status=ack.status),
                checkpoint_id=rp.checkpoint
            )

    def placeQuoteResponseFIX(self, request: PlaceMessageRequestTyped, context) -> PlaceMessageResponseTyped:
        logger.debug(f'placeQuoteResponseFIX received request: {MessageToString(request.metadata, as_one_line=True)}')

        grpc_method_attrs = GrpcMethodAttributes(method_name='Place quote response FIX',
                                                 request_event_id=request.parent_event_id,
                                                 request_description=request.description,
                                                 context=context)

        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=self.heartbeat_prefilter) as rp:
            request_msg: Message = req.create_message(request)
            rp.send(request_msg)

            quote_report_filter = lambda response_msg: \
                response_msg['RFQID'] == request_msg['RFQID'] \
                and response_msg.metadata.message_type in {'ExecutionReport', 'QuoteStatusReport'}

            report: ActMessage = rp.receive_first_matching(pass_on=quote_report_filter)

            return PlaceMessageResponseTyped(status=RequestStatus(status=report.status),
                                             checkpoint_id=rp.checkpoint)

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

        # Start RequestProcessor context manager with the alias 'rp'
        with RequestProcessor(self.handler_attrs, grpc_method_attrs, prefilter=prefilter) as rp:
            # Get th2-message from request, using custom converter method
            request_msg: Message = req.create_security_list_request(request)
            rp.send(request_msg)  # Send message, no echo will be received

            # Describe filter for SecurityList message with LastFragment == true field as lambda
            last_fragment_filter = lambda response_msg: response_msg['LastFragment'] == 'true'

            # All matching messages before LastFragment message (remember that we prefiltered SecurityList already)
            security_list: List[ActMessage] = rp.receive_all_before_matching(pass_on=last_fragment_filter,
                                                                             timeout=20)

            # Form response for script, using custom converter method
            return PlaceSecurityListResponse(
                securityListDictionary=resp.create_security_list_dictionary(security_list),
                status=RequestStatus(status=security_list[0].status),
                checkpoint_id=rp.checkpoint
            )
