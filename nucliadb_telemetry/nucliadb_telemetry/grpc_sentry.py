# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import functools
from typing import Any, Awaitable, Callable

from grpc import HandlerCallDetails, RpcMethodHandler
from grpc.experimental import aio  # type: ignore
from grpc.experimental import wrap_server_method_handler  # type: ignore

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
