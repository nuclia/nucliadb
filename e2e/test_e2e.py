import pytest
import requests
import os
import random

BASE_URL = os.environ.get("NUCLIADB_URL", "http://localhost:8080")


@pytest.fixture(scope="session")
def kbid():
    # generate random slug
    slug = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=10))
    resp = requests.post(
        os.path.join(BASE_URL, "api/v1/kbs"),
        headers={"content-type": "application/json", "X-NUCLIADB-ROLES": "MANAGER"},
        json={"slug": slug, "zone": "local", "title": slug},
    )
    resp.raise_for_status()
    return resp.json()["id"]


@pytest.fixture(scope="session")
def resource_id(kbid: str):
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/resources"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "WRITER",
            "x-synchronous": "true",
            "x-ndb-client": "web",
        },
        json={
            "links": {"link": {"uri": "https://en.wikipedia.org/wiki/Cricket"}},
            "usermetadata": {"classifications": []},
            "title": "https://en.wikipedia.org/wiki/Cricket",
            "icon": "application/stf-link",
            "origin": {"url": "https://en.wikipedia.org/wiki/Cricket"},
        },
    )

    resp.raise_for_status()

    return resp.json()["id"]


def test_nodes_ready(kbid: str):
    resp = requests.get(os.path.join(BASE_URL, f"api/v1/cluster/nodes"))
    resp.raise_for_status()
    assert len(resp.json()) == 2


def test_resource_processed(kbid: str, resource_id: str):
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/resources"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "WRITER",
            "x-synchronous": "true",
            "x-ndb-client": "web",
        },
        json={
            "links": {"link": {"uri": "https://en.wikipedia.org/wiki/Cricket"}},
            "usermetadata": {"classifications": []},
            "title": "https://en.wikipedia.org/wiki/Cricket",
            "icon": "application/stf-link",
            "origin": {"url": "https://en.wikipedia.org/wiki/Cricket"},
        },
    )

    resp.raise_for_status()
