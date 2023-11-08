import json
import os
from dataclasses import dataclass, field
from functools import cache
from typing import Any, Optional

Search = dict[str, Any]

CURRENT_DIR = os.getcwd()
SAVED_SEARCHES_FILE = "saved_searches.json"


@dataclass
class SavedSearch:
    search: Search = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    description: Optional[str] = None


@cache
def load_kb_saved_searches(kbid_or_slug: str, with_tags=None) -> list[Search]:
    try:
        path = f"{CURRENT_DIR}/{SAVED_SEARCHES_FILE}"
        with open(path, "r") as f:
            requests = json.loads(f.read())
        saved_searches = [SavedSearch(**ss) for ss in requests[kbid_or_slug]]
        if with_tags is None:
            return [ss.search for ss in saved_searches]
        else:
            return [
                ss.search
                for ss in saved_searches
                if set(ss.tags).intersection(set(with_tags)) != set()
            ]
    except (FileNotFoundError, KeyError):
        return []
