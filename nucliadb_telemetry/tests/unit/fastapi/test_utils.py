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
