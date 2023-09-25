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
from typing import Union

from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.async_tasks import (
    AsyncTasksDataManager,
    Task,
    TaskNotFoundError,
    TaskStatus,
)
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.models.responses import HTTPClientError
from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/task/{{task_id}}",
    status_code=200,
    name="Cancel an async task",
    tags=["Knowledge Boxes"],
    response_model=None,
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def cancel_task_endpoint(
    request: Request, kbid: str, task_id: str
) -> Union[HTTPClientError, None]:
    context = get_app_context(request.app)
    dm = AsyncTasksDataManager(context.kv_driver)
    try:
        task: Task = await dm.get_task(kbid, task_id)
        task.status = TaskStatus.CANCELLED
        await dm.set_task(task)
        return None
    except TaskNotFoundError:
        return HTTPClientError(
            status_code=404,
            detail="Task not found",
        )
