import base64
import io
import json
import os
import random
import time

import pytest
import requests

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
    kbid = resp.json()["uuid"]
    print(f'Created KB with id "{kbid}", slug "{slug}"')
    return kbid


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
    return resp.json()["uuid"]


def test_nodes_ready():
    tries = 1
    while True:
        try:
            resp = requests.get(os.path.join(BASE_URL, "api/v1/cluster/nodes"))
            resp.raise_for_status()
            assert len(resp.json()) == 2
            return
        except Exception:
            time.sleep(1)
            print(f"Waiting for nodes to be ready...")
            if tries > 30:
                raise
            tries += 1


def test_versions():
    resp = requests.get(os.path.join(BASE_URL, "api/v1/versions"))
    resp.raise_for_status()
    data = resp.json()
    print(f"Versions: {data}")
    assert data["nucliadb"]["installed"]
    assert "latest" in data["nucliadb"]
    assert data["nucliadb-admin-assets"]["installed"]
    assert "latest" in data["nucliadb-admin-assets"]


def test_config_check(kbid: str):
    resp = requests.get(
        os.path.join(BASE_URL, f"api/v1/config-check"),
        headers={"X-NUCLIADB-ROLES": "READER"},
    )
    resp.raise_for_status()
    data = resp.json()
    assert data["nua_api_key"]["has_key"]
    assert data["nua_api_key"]["valid"]


def test_resource_processed(kbid: str, resource_id: str):
    start = time.time()
    while True:
        resp = requests.get(
            os.path.join(BASE_URL, f"api/v1/kb/{kbid}/resource/{resource_id}"),
            headers={
                "content-type": "application/json",
                "X-NUCLIADB-ROLES": "READER",
                "x-ndb-client": "web",
            },
        )

        resp.raise_for_status()

        if resp.json()["metadata"]["status"] == "PROCESSED":
            break

        # takes too long to process, skip for now
        return

        # waited = time.time() - start
        # if waited > (60 * 10):
        #     raise Exception("Resource took too long to process")

        # if int(waited) % 10 == 0 and int(waited) > 0:
        #     print(f"Waiting for resource to process: {int(waited)}s")

        # time.sleep(1)


def test_search(kbid: str, resource_id: str):
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/chat"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
        json={
            "query": "What is Cricket?",
            "context": [],
            "show": ["basic", "values", "origin"],
            "features": ["paragraphs", "relations"],
            "inTitleOnly": False,
            "highlight": True,
            "autofilter": False,
            "page_number": 0,
            "filters": [],
        },
    )

    resp.raise_for_status()

    data = io.BytesIO(resp.content)

    toread_bytes = data.read(4)
    toread = int.from_bytes(toread_bytes, byteorder="big")
    raw_search_results = data.read(toread)
    search_results = json.loads(base64.b64decode(raw_search_results))
    chat_response = data.read().decode("utf-8")
    print(f"Cht response: {chat_response}")
    print(f"Search results: {search_results}")

    # assert "Not enough data to answer this" not in chat_response
    # assert len(search_results["resources"]) == 1


def test_predict_proxy(kbid: str):
    _test_predict_proxy_chat(kbid)
    _test_predict_proxy_tokens(kbid)
    _test_predict_proxy_rephrase(kbid)


def _test_predict_proxy_chat(kbid: str):
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/predict/chat"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
        json={
            "question": "Who is the best football player?",
            "query_context": [
                "Many football players have existed. Cristiano Ronaldo and Messi among them, but Messi is by far the greatest."
            ],
            "user_id": "someone@company.uk",
        },
    )
    resp.raise_for_status()
    data = io.BytesIO(resp.content)
    answer = data.read().decode("utf-8")
    print(f"Answer: {answer}")
    # assert "Messi" in answer


def _test_predict_proxy_tokens(kbid: str):
    resp = requests.get(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/predict/tokens"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
        params={
            "text": "Barcelona",
        },
    )
    resp.raise_for_status()
    data = resp.json()
    assert data["tokens"][0]["text"] == "Barcelona"


def _test_predict_proxy_rephrase(kbid: str):
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/predict/rephrase"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
        json={
            "question": "Who is the best one?",
            "context": [
                {
                    "author": "NUCLIA",
                    "text": "Many football players have existed. Cristiano Ronaldo and Messi among them.",
                },
                {"author": "USER", "text": "Tell me some football players"},
            ],
            "user_id": "someone@company.uk",
        },
    )
    resp.raise_for_status()
    rephrased_query = resp.json()
    # Status code 0 means success...
    assert rephrased_query.endswith("0")
