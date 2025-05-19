# Copyright 2025 Bosutech XXI S.L.
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
#

import functools
from typing import Any, Awaitable, Callable

from grpc import HandlerCallDetails, RpcMethodHandler
from grpc.experimental import (  # type: ignore
    aio,
    wrap_server_method_handler,
)

from nucliadb_telemetry.errors import capture_exception


class SentryInterceptor(aio.ServerInterceptor):
    async def intercept_service(
        self,
        continuation: Callable[[HandlerCallDetails], Awaitable[RpcMethodHandler]],
        handler_call_details: HandlerCallDetails,
    ) -> RpcMethodHandler:
        handler = await continuation(handler_call_details)

        def wrapper_stream(behavior: Callable[[Any, aio.ServicerContext], Any]):
            @functools.wraps(behavior)
            async def wrapper(request: Any, context: aio.ServicerContext) -> Any:
                # And now we run the actual RPC.
                try:
                    grpc_method = behavior(request, context)
                    async for value in grpc_method:
                        yield value
                except Exception as error:
                    capture_exception(error)
                    raise error

            return wrapper

        def wrapper_unary(behavior: Callable[[Any, aio.ServicerContext], Any]):
            @functools.wraps(behavior)
            async def wrapper(request: Any, context: aio.ServicerContext) -> Any:
                # And now we run the actual RPC.
                try:
                    return await behavior(request, context)
                except Exception as error:
                    capture_exception(error)
                    raise error

            return wrapper

        if handler.response_streaming:
            return wrap_server_method_handler(wrapper_stream, handler)
        else:
            return wrap_server_method_handler(wrapper_unary, handler)
