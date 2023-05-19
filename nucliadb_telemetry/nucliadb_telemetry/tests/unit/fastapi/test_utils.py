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
from fastapi import FastAPI

from nucliadb_telemetry.fastapi import utils

app = FastAPI()


@app.get("/api/v1/kb/{kbid}")
def get_kb(kbid: str):
    return {"kbid": kbid}


def test_get_path_template():
    scope = {
        "app": app,
        "path": "/api/v1/kb/123",
        "method": "GET",
        "type": "http",
    }

    path_template = utils.get_path_template(scope)
    assert path_template.path == "/api/v1/kb/{kbid}"
    assert path_template.match is True
    assert path_template.scope["path_params"] == {"kbid": "123"}
