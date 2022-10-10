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

import datetime
import json
import sys

from fastapi.openapi.utils import get_openapi
from starlette.routing import Mount

from nucliadb.reader import API_PREFIX


def is_versioned_route(route):
    return isinstance(route, Mount) and route.path.startswith(f"/{API_PREFIX}/v")


def extract_openapi(application, version, commit_id):
    app = [
        route.app
        for route in application.routes
        if is_versioned_route(route) and route.app.version == version
    ][0]
    document = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        terms_of_service=app.terms_of_service,
        contact=app.contact,
        license_info=app.license_info,
        routes=app.routes,
        tags=app.openapi_tags,
        servers=app.servers,
    )

    document["x-metadata"] = {
        "nucliadb_reader": {
            "commit": commit_id,
            "last_updated": datetime.datetime.utcnow().isoformat(),
        }
    }
    return document


def command_extract_openapi():
    from nucliadb.reader.app import application

    openapi_json_path = sys.argv[1]
    api_version = sys.argv[2]
    commit_id = sys.argv[3]

    json.dump(
        extract_openapi(application, api_version, commit_id),
        open(openapi_json_path, "w"),
    )
