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
#

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from starlette.routing import Mount

from nucliadb.common.context import ApplicationContext


@asynccontextmanager
async def inject_app_context(app: FastAPI, context: Optional[ApplicationContext] = None):
    if context is None:
        context = ApplicationContext()

    app.state.context = context

    # Need to add app context in all sub-applications
    for route in app.router.routes:
        if isinstance(route, Mount) and isinstance(route.app, FastAPI):
            route.app.state.context = context

    await context.initialize()
    yield context
    await context.finalize()


def get_app_context(application: FastAPI) -> ApplicationContext:
    return application.state.context
