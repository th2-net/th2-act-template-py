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

import logging.config
import sys

from th2_act import Act, ActServer
from th2_common.schema.factory.common_factory import CommonFactory

logger = logging.getLogger()


def shutdown_hook(type, value, traceback):  # noqa: A002
    try:
        logger.info('Act is terminating')
        GRPCServer.stop()
        factory.close()
    except Exception as e:
        logger.error('GRPC server shutdown was interrupted', e)
    finally:
        logger.info('Act terminated')


factory = CommonFactory()

grpc_router = factory.grpc_router
message_batch_router = factory.message_parsed_batch_router
event_batch_router = factory.event_batch_router

grpc_server = grpc_router.server
act = Act(grpc_router, message_batch_router, event_batch_router)

GRPCServer = ActServer(grpc_server, act.handlers)

sys.excepthook = shutdown_hook

GRPCServer.start()
