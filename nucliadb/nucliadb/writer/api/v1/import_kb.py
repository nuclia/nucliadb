from typing import Annotated

from fastapi import Depends
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.export_import.fastapi import FastAPIExportStream, get_importer_context
from nucliadb.export_import.importer import import_kb
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one


async def context(request: Request) -> dict:
    return {"importer": get_importer_context(request.app)}


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/import",
    status_code=200,
    name="Import to a Knowledge Box",
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def import_kb_endpoint(
    request: Request, kbid: str, context: Annotated[dict, Depends(context)]
) -> None:
    await import_kb(
        context=context["importer"],
        kbid=kbid,
        stream=FastAPIExportStream(request),
    )
