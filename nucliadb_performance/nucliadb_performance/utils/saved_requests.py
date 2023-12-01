from functools import cache
from typing import Any, Optional, Union

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
    saved_requests_file: str, kbid_or_slug: str, endpoint: str, with_tags=None
) -> list[Request]:
    saved_requests = load_all_saved_requests(saved_requests_file)
    kb_requests = []
    for rs in saved_requests.sets.values():
        if kbid_or_slug not in rs.kbs:
            continue
        kb_requests.extend([r for r in rs.requests if r.endpoint == endpoint])
    if with_tags is None:
        return [kb_req.request for kb_req in kb_requests]
    else:
        return [
            kb_req.request
            for kb_req in kb_requests
            if set(with_tags).issubset(kb_req.tags)
        ]


@cache
def load_all_saved_requests(saved_requests_file: str) -> SavedRequests:
    return SavedRequests.parse_file(saved_requests_file)
