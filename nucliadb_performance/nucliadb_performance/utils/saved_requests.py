from functools import cache
from typing import Any, Optional

from pydantic import BaseModel

Payload = dict[str, Any]


class Request(BaseModel):
    url: str
    method: str
    payload: Optional[Payload] = None


class SavedRequest(BaseModel):
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
    saved_requests_file: str, kbid_or_slug: str, with_tags=None
) -> list[Request]:
    try:
        saved_requests = SavedRequests.parse_file(saved_requests_file)
        kb_requests = []
        for rs in saved_requests.sets.values():
            if kbid_or_slug not in rs.kbs:
                continue
            kb_requests.extend(rs.requests)

        if with_tags is None:
            return [kb_req.request for kb_req in kb_requests]
        else:
            return [
                kb_req.request
                for kb_req in kb_requests
                if set(with_tags).issubset(kb_req.tags)
            ]
    except (FileNotFoundError, KeyError):
        return []
