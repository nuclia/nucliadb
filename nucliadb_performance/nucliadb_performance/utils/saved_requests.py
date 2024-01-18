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

from functools import cache
from typing import Any, Optional, Union

try:
    from pydantic.v1 import BaseModel
except ImportError:
    from pydantic import BaseModel

Payload = dict[str, Any]
Params = dict[str, Union[str, list[str]]]


class Request(BaseModel):
    url: str
    method: str
    payload: Optional[Payload] = None
    params: Optional[Params] = None


class SavedRequest(BaseModel):
    endpoint: str
    request: Request
    tags: list[str] = []
    description: Optional[str] = None


class SavedRequestsSet(BaseModel):
    kbs: list[str]
    requests: list[SavedRequest]


class SavedRequests(BaseModel):
    sets: dict[str, SavedRequestsSet]


@cache
def load_saved_request(
    saved_requests_file: str, kbid_or_slug: str, endpoint: str
) -> list[Request]:
    saved_requests = load_all_saved_requests(saved_requests_file)
    kb_requests = []
    for rs in saved_requests.sets.values():
        if kbid_or_slug not in rs.kbs:
            continue
        kb_requests.extend([r for r in rs.requests if r.endpoint == endpoint])
    return [kb_req.request for kb_req in kb_requests]


@cache
def load_all_saved_requests(saved_requests_file: str) -> SavedRequests:
    return SavedRequests.parse_file(saved_requests_file)
