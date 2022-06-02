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

from th2_act import ActConnector, ActParameters, RequestProcessor
from th2_act_template.custom import request_convertors as req
from th2_act_template.custom import response_convertors as resp
from th2_act_template.custom.support_functions import create_security_list_dictionary
from th2_grpc_act_template import act_template_typed_pb2_grpc
from th2_grpc_act_template.act_template_pb2 import PlaceSecurityListResponse, SendMessageResponse
from th2_grpc_act_template.act_template_typed_pb2 import PlaceMessageMultipleResponseTyped
from th2_grpc_common.common_pb2 import RequestStatus

logger = logging.getLogger()


class ActHandler(act_template_typed_pb2_grpc.ActTypedServicer):

    def __init__(self, act_conn: ActConnector):
        self.act_conn = act_conn

    def placeOrderFIX(self, request, context):
        act_parameters = ActParameters(act_name='Place order FIX',
                                       request_event_id=request.parent_event_id,
                                       request_description=request.description,
                                       context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_conn, act_parameters, prefilter=prefilter) as rp:
            request_msg = req.create_new_order_single(request)
            rp.send(request_msg)

            execution_report_filter = lambda response_msg: (
                response_msg['ClOrdID'] == request_msg['ClOrdID']
                and response_msg.metadata.message_type == 'ExecutionReport'
            )

            business_reject_filter = lambda response_msg: (
                response_msg['BusinessRejectRefID'] == request_msg['ClOrdID']
                and response_msg.metadata.message_type == 'BusinessMessageReject'
            )

            act_response = rp.receive_first_matching(
                message_filters={
                    execution_report_filter: RequestStatus.SUCCESS,
                    business_reject_filter: RequestStatus.ERROR
                }
            )

        return resp.act_response_to_typed_response(act_response)

    def sendMessage(self, request, context):
        act_parameters = ActParameters(act_name='Send message',
                                       request_event_id=request.parent_event_id,
                                       request_description=request.description,
                                       context=context)

        with RequestProcessor(self.act_conn, act_parameters) as rp:
            request_msg = req.create_message(request)
            act_response = rp.send(request_msg)

        return SendMessageResponse(status=act_response.status,
                                   checkpoint_id=act_response.checkpoint)

    def placeQuoteRequestFIX(self, request, context):
        act_parameters = ActParameters(act_name='Place quote request FIX',
                                       request_event_id=request.parent_event_id,
                                       request_description=request.description,
                                       context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_conn, act_parameters, prefilter=prefilter) as rp:
            request_msg = req.create_message(request)
            rp.send(request_msg)

            message_filter = lambda response_msg: (
                response_msg['QuoteReqID'] == request_msg['QuoteReqID']
                and response_msg.metadata.message_type == 'QuoteStatusReport'
            )

            act_response = rp.receive_first_matching(message_filters={message_filter: RequestStatus.SUCCESS})

        typed_response = resp.act_response_to_typed_response(act_response)

        return typed_response

    def placeQuoteFIX(self, request, context):
        act_parameters = ActParameters(act_name='Place quote FIX',
                                       request_event_id=request.parent_event_id,
                                       request_description=request.description,
                                       context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_conn, act_parameters, prefilter=prefilter) as rp:
            request_msg = req.create_quote(request)
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
                timeout=10)

            quote_filter = lambda response_msg: (
                response_msg['Symbol'] == request_msg['Symbol']
                and response_msg['NoQuoteQualifiers'][0]['QuoteQualifier'] == 'R'
                and response_msg['QuoteType'] == '0'
            )

            quote_act_responses = rp.receive_all_matching(
                message_filters={quote_filter: RequestStatus.SUCCESS},
                wait_time=5
            )

        quote_status_report = resp.act_response_to_typed_response(quote_status_report_act_response)

        quotes = [resp.act_response_to_typed_response(act_response) for act_response in quote_act_responses]

        return PlaceMessageMultipleResponseTyped(
            place_message_response_typed=[quote_status_report, *quotes]
        )

    def placeOrderMassCancelRequestFIX(self, request, context):
        act_parameters = ActParameters(act_name='Place order mass cancel request FIX',
                                       request_event_id=request.parent_event_id,
                                       request_description=request.description,
                                       context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_conn, act_parameters, prefilter=prefilter) as rp:
            request_msg = req.create_message(request)
            rp.send(request_msg)

            message_filter = lambda response_msg: (
                response_msg['ClOrdID'] == request_msg['ClOrdID']
                and response_msg.metadata.message_type == 'OrderMassCancelReport'
            )

            act_response = rp.receive_first_matching(message_filters={message_filter: RequestStatus.SUCCESS})

        typed_response = resp.act_response_to_typed_response(act_response)

        return typed_response

    def placeQuoteCancelFIX(self, request, context):
        act_parameters = ActParameters(act_name='Place quote cancel FIX',
                                       request_event_id=request.parent_event_id,
                                       request_description=request.description,
                                       context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_conn, act_parameters, prefilter=prefilter) as rp:
            request_msg = req.create_message(request)
            rp.send(request_msg)

            message_filter = lambda response_msg: (
                response_msg['QuoteID'] == request_msg['QuoteMsgID']
                and response_msg.metadata.message_type == 'MassQuoteAcknowledgement'
            )

            act_response = rp.receive_first_matching(message_filters={message_filter: RequestStatus.SUCCESS})

        typed_response = resp.act_response_to_typed_response(act_response)

        return typed_response

    def placeQuoteResponseFIX(self, request, context):
        act_parameters = ActParameters(act_name='Place quote response FIX',
                                       request_event_id=request.parent_event_id,
                                       request_description=request.description,
                                       context=context)

        prefilter = lambda message: message.metadata.message_type != 'Heartbeat'

        with RequestProcessor(self.act_conn, act_parameters, prefilter=prefilter) as rp:
            request_msg = req.create_message(request)
            rp.send(request_msg)

            message_filter = lambda response_msg: (
                response_msg['RFQID'] == request_msg['RFQID']
                and response_msg.metadata.message_type in {'ExecutionReport', 'QuoteStatusReport'}
            )

            act_response = rp.receive_first_matching(message_filters={message_filter: RequestStatus.SUCCESS})

        typed_response = resp.act_response_to_typed_response(act_response)

        return typed_response

    def placeSecurityListRequest(self, request, context):
        act_parameters = ActParameters(act_name='Place security list request',
                                       request_event_id=request.parent_event_id,
                                       request_description=request.description,
                                       context=context)

        request_msg = req.create_security_list_request(request)
        prefilter = lambda incoming_message: (
            incoming_message.metadata.message_type == 'SecurityList'
            and incoming_message['SecurityReqID'] == request_msg['SecurityReqID']
        )

        with RequestProcessor(self.act_conn, act_parameters, prefilter=prefilter) as rp:
            rp.send(request_msg)

            message_filter = lambda response_msg: response_msg['LastFragment'] == 'true'

            act_multi_response = rp.receive_all_before_matching(message_filters={message_filter: RequestStatus.SUCCESS},
                                                                timeout=20)

        return PlaceSecurityListResponse(securityListDictionary=create_security_list_dictionary(act_multi_response),
                                         status=act_multi_response.status,
                                         checkpoint_id=act_multi_response.checkpoint)
