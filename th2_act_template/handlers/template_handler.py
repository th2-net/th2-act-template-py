# Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

from google.protobuf.text_format import MessageToString
from th2_act import GrpcMethodAttributes, HandlerAttributes, RequestProcessor
import th2_act_template.custom.response_convertors as resp
from th2_grpc_act_template import act_template_pb2_grpc
from th2_grpc_act_template.act_template_pb2 import PlaceMessageMultipleResponse, PlaceMessageRequest, \
    PlaceMessageResponse, PlaceSecurityListResponse, SendMessageResponse
from th2_grpc_common.common_pb2 import RequestStatus

logger = logging.getLogger()


class ActHandler(act_template_pb2_grpc.ActServicer):

    def __init__(self, act_attrs: HandlerAttributes):
        self.act_attrs = act_attrs

    def placeOrderFIX(self, request: PlaceMessageRequest, context) -> PlaceMessageResponse:
        logger.debug(f'placeOrderFIX received request: {MessageToString(request.message.metadata, as_one_line=True)}')

        method_attrs = GrpcMethodAttributes(method_name='Place order FIX',
                                            request_event_id=request.parent_event_id,
                                            request_description=request.description,
                                            context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_attrs, method_attrs, prefilter=prefilter) as rp:
            request_msg = request.message
            request_msg_sequence = rp.send(request_msg, echo_key_field='ClOrdID')

            execution_report_filter = lambda response_msg: (
                    response_msg['ClOrdID'] == request_msg['ClOrdID']
                    and response_msg.metadata.message_type == 'ExecutionReport'
            )

            business_reject_filter = lambda response_msg: (
                    response_msg['BusinessRejectRefID'] == request_msg['ClOrdID']
                    and response_msg.metadata.message_type == 'BusinessMessageReject'
            )

            system_reject_filter = lambda response_msg: (
                    response_msg['RefSeqNum'] == request_msg_sequence
            )

            act_response = rp.receive_first_matching(
                message_filters={
                    execution_report_filter: RequestStatus.SUCCESS,
                    business_reject_filter: RequestStatus.ERROR,
                    system_reject_filter: RequestStatus.ERROR
                },
                timeout=10)

        return PlaceMessageResponse(checkpoint_id=act_response.checkpoint,
                                    status=act_response.status)

    def sendMessage(self, request: PlaceMessageRequest, context) -> SendMessageResponse:
        logger.debug(f'sendMessage received request: {MessageToString(request.message.metadata, as_one_line=True)}')

        act_parameters = GrpcMethodAttributes(method_name='Send message',
                                              request_event_id=request.parent_event_id,
                                              request_description=request.description,
                                              context=context)

        with RequestProcessor(self.act_attrs, act_parameters) as rp:
            request_msg = request.message
            rp.send(request_msg)

        return SendMessageResponse()

    def placeQuoteRequestFIX(self, request: PlaceMessageRequest, context) -> PlaceMessageResponse:
        logger.debug(f'placeQuoteRequestFIX received request: '
                     f'{MessageToString(request.message.metadata, as_one_line=True)}')

        act_parameters = GrpcMethodAttributes(method_name='Place quote request FIX',
                                              request_event_id=request.parent_event_id,
                                              request_description=request.description,
                                              context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_attrs, act_parameters, prefilter=prefilter) as rp:
            request_msg = request.message
            rp.send(request_msg)

            message_filter = lambda response_msg: (
                    response_msg['QuoteReqID'] == request_msg['QuoteReqID']
                    and response_msg.metadata.message_type == 'QuoteStatusReport'
            )

            act_response = rp.receive_first_matching(message_filters={message_filter: RequestStatus.SUCCESS})

        return PlaceMessageResponse(response_message=act_response.message,
                                    status=act_response.status,
                                    checkpoint_id=act_response.checkpoint)

    def placeQuoteFIX(self, request: PlaceMessageRequest, context) -> PlaceMessageMultipleResponse:
        logger.debug(f'placeQuoteFIX received request: {MessageToString(request.message.metadata, as_one_line=True)}')

        act_parameters = GrpcMethodAttributes(method_name='Place quote FIX',
                                              request_event_id=request.parent_event_id,
                                              request_description=request.description,
                                              context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_attrs, act_parameters, prefilter=prefilter) as rp:
            request_msg = request.message
            rp.send(request_msg)

            quote_status_report_accepted_filter = lambda response_msg: (
                    response_msg['QuoteID'] == request_msg['QuoteID']
                    and response_msg.metadata.message_type == 'QuoteStatusReport'
                    and response_msg['QuoteStatus'] == '0'
            )

            quote_status_report_rejected_filter = lambda response_msg: (
                    response_msg['QuoteID'] == request_msg['QuoteID']
                    and response_msg.metadata.message_type == 'QuoteStatusReport'
                    and response_msg['QuoteStatus'] == '5'
            )

            quote_status_report_act_response = rp.receive_first_matching(
                message_filters={
                    quote_status_report_accepted_filter: RequestStatus.SUCCESS,
                    quote_status_report_rejected_filter: RequestStatus.ERROR
                },
                timeout=10
            )

            quote_filter = lambda response_msg: (
                    response_msg['Symbol'] == request_msg['Symbol']
                    and response_msg['NoQuoteQualifiers'][0]['QuoteQualifier'] == 'R'
                    and response_msg['QuoteType'] == '0'
            )

            quote_act_responses = rp.receive_all_matching(
                message_filters={quote_filter: RequestStatus.SUCCESS},
                wait_time=5)

        responses = []
        if quote_status_report_act_response.message is not None:
            responses.append(quote_status_report_act_response.message)
        responses.extend(act_response.message for act_response in quote_act_responses
                         if act_response.message is not None)

        return PlaceMessageMultipleResponse(
            response_message=responses,
            checkpoint_id=quote_status_report_act_response.checkpoint,
            status=quote_status_report_act_response.status
        )

    def placeOrderMassCancelRequestFIX(self, request: PlaceMessageRequest, context) -> PlaceMessageResponse:
        logger.debug(f'placeOrderMassCancelRequestFIX received request: '
                     f'{MessageToString(request.message.metadata, as_one_line=True)}')

        act_parameters = GrpcMethodAttributes(method_name='Place order mass cancel request FIX',
                                              request_event_id=request.parent_event_id,
                                              request_description=request.description,
                                              context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_attrs, act_parameters, prefilter=prefilter) as rp:
            request_msg = request.message
            rp.send(request_msg)

            message_filter = lambda response_msg: (
                    response_msg['ClOrdID'] == request_msg['ClOrdID']
                    and response_msg.metadata.message_type == 'OrderMassCancelReport'
            )

            act_response = rp.receive_first_matching(message_filters={message_filter: RequestStatus.SUCCESS})

        return PlaceMessageResponse(response_message=act_response.message,
                                    status=act_response.status,
                                    checkpoint_id=act_response.checkpoint)

    def placeQuoteCancelFIX(self, request: PlaceMessageRequest, context) -> PlaceMessageResponse:
        logger.debug(f'placeQuoteCancelFIX received request: '
                     f'{MessageToString(request.message.metadata, as_one_line=True)}')

        act_parameters = GrpcMethodAttributes(method_name='Place quote cancel FIX',
                                              request_event_id=request.parent_event_id,
                                              request_description=request.description,
                                              context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_attrs, act_parameters, prefilter=prefilter) as rp:
            request_msg = request.message
            rp.send(request_msg)

            message_filter = lambda response_msg: (
                    response_msg['QuoteID'] == request_msg['QuoteMsgID']
                    and response_msg.metadata.message_type == 'MassQuoteAcknowledgement'
            )

            act_response = rp.receive_first_matching(message_filters={message_filter: RequestStatus.SUCCESS})

        return PlaceMessageResponse(response_message=act_response.message,
                                    status=act_response.status,
                                    checkpoint_id=act_response.checkpoint)

    def placeQuoteResponseFIX(self, request: PlaceMessageRequest, context) -> PlaceMessageResponse:
        logger.debug(f'placeQuoteResponseFIX received request: '
                     f'{MessageToString(request.message.metadata, as_one_line=True)}')

        act_parameters = GrpcMethodAttributes(method_name='Place quote response FIX',
                                              request_event_id=request.parent_event_id,
                                              request_description=request.description,
                                              context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_attrs, act_parameters, prefilter=prefilter) as rp:
            request_msg = request.message
            rp.send(request_msg)

            message_filter = lambda response_msg: (
                    response_msg['RFQID'] == request_msg['RFQID']
                    and response_msg.metadata.message_type in ['ExecutionReport', 'QuoteStatusReport']
            )

            act_response = rp.receive_first_matching(message_filters={message_filter: RequestStatus.SUCCESS})

        return PlaceMessageResponse(response_message=act_response.message,
                                    status=act_response.status,
                                    checkpoint_id=act_response.checkpoint)

    def placeSecurityListRequest(self, request: PlaceMessageRequest, context) -> PlaceSecurityListResponse:
        logger.debug(f'placeSecurityListRequest received request: '
                     f'{MessageToString(request.message.metadata, as_one_line=True)}')

        act_parameters = GrpcMethodAttributes(method_name='Place security list request',
                                              request_event_id=request.parent_event_id,
                                              request_description=request.description,
                                              context=context)

        prefilter = lambda incoming_message: (
                incoming_message.metadata.message_type == 'SecurityList'
                and incoming_message['SecurityReqID'] == request.message['SecurityReqID']
        )

        with RequestProcessor(self.act_attrs, act_parameters, prefilter) as rp:
            request_msg = request.message
            rp.send(request_msg)

            message_filter = lambda response_msg: response_msg['LastFragment'] == 'true'

            act_multi_response = rp.receive_all_before_matching(message_filters={message_filter: RequestStatus.SUCCESS},
                                                                timeout=20)

        return PlaceSecurityListResponse(
            securityListDictionary=resp.create_security_list_dictionary(act_multi_response.messages),
            status=act_multi_response.status,
            checkpoint_id=act_multi_response.checkpoint
        )
