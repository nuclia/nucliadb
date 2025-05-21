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
    raise_for_status(resp)
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
            "x-ndb-client": "web",
        },
        json={
            "usermetadata": {"classifications": []},
            "title": "Soccer",
            "texts": {
                "text1": {
                    "body": """The term "soccer" is derived from the words "association" and "-er". The formal name of the sport is "association football", but students at Oxford University in the 1870s began to shorten the name by removing the first and last three syllables, and adding "-er". For example, "breakfast" became "brekker" and "rugby" became "rugger". The term "soccer" was first recorded in 1891.""",  # noqa
                    "format": "PLAIN",
                }
            },
        },
    )

    raise_for_status(resp)
    return resp.json()["uuid"]


def test_versions():
    resp = requests.get(os.path.join(BASE_URL, "api/v1/versions"))
    raise_for_status(resp)
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
    raise_for_status(resp)
    data = resp.json()
    assert data["nua_api_key"]["has_key"]
    assert data["nua_api_key"]["valid"]


def test_resource_processed(kbid: str, resource_id: str):
    wait_for_resource_processed(kbid, resource_id)


def wait_for_resource_processed(kbid: str, resource_id: str):
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

        raise_for_status(resp)

        if resp.json()["metadata"]["status"] == "PROCESSED":
            break

        waited = time.time() - start
        if waited > (60 * 10):
            raise Exception("Resource took too long to process")

        if int(waited) % 20 == 0 and int(waited) > 0:
            print(f"Waiting for resource to process: {int(waited)}s")

        time.sleep(5)


def test_b64_file_upload(kbid: str):
    # Create a resource with an image embedded as base64 in the payload
    image = b"iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAABjElEQVR42mNk"
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/resources"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "WRITER",
            "x-ndb-client": "web",
        },
        json={
            "files": {
                "image": {
                    "file": {
                        "filename": "image.png",
                        "content_type": "image/png",
                        "payload": base64.b64encode(image).decode("utf-8"),
                    }
                }
            }
        },
    )
    raise_for_status(resp)
    resource_id = resp.json()["uuid"]

    # Check that the resource is processed without errors
    wait_for_resource_processed(kbid, resource_id)

    # Check that the image can be downloaded and is the same as the one uploaded
    resp = requests.get(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/resource/{resource_id}/file/image/download/field"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
    )
    raise_for_status(resp)
    assert resp.content == image


def test_search(kbid: str, resource_id: str):
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/ask"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
            "x-synchronous": "true",
        },
        json={
            "query": "Why is soccer called soccer?",
            "context": [],
            "show": ["basic", "values", "origin"],
            "features": ["keyword", "relations"],
            "highlight": True,
            "autofilter": False,
            "page_number": 0,
            "filters": [],
            "debug": True,
        },
    )

    raise_for_status(resp)
    ask_response = resp.json()
    print(f"Search results: {ask_response['retrieval_results']}")
    assert len(ask_response["retrieval_results"]["resources"]) == 1
    print(f"Relations payload: {ask_response['relations']}")
    print(f"Answer: {ask_response['answer']}")
    print(f"Citations: {ask_response['citations']}")


def test_predict_proxy(kbid: str):
    _test_predict_proxy_chat(kbid)
    _test_predict_proxy_tokens(kbid)
    _test_predict_proxy_rephrase(kbid)


def test_learning_config(kbid: str):
    # Try to patch some config
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/configuration"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "WRITER",
            "x-ndb-client": "web",
        },
        json={
            "foo": "bar",
        },
    )
    assert resp.status_code == 422

    # Get config
    resp = requests.get(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/configuration"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
    )
    assert resp.status_code == 200

    # Get the schema
    resp = requests.get(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/schema"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
    )
    raise_for_status(resp)

    # Get the models
    resp = requests.get(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/models"),
        headers={
            "content-type": "application/json",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
    )
    raise_for_status(resp)


def test_mimetypes(kbid: str):
    # Create a resource with a few different mimetypes to ensure they are supported
    # These depend on /etc/mime.types being available in the container image

    MIMETYPES = [
        (".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        (".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        (".ods", "application/vnd.oasis.opendocument.spreadsheet"),
        (".epub", "application/epub+zip"),
        (".scad", "application/x-openscad"),
    ]

    for extension, mimetype in MIMETYPES:
        resp = requests.post(
            os.path.join(BASE_URL, f"api/v1/kb/{kbid}/resources"),
            headers={
                "content-type": "application/json",
                "X-NUCLIADB-ROLES": "WRITER",
                "x-ndb-client": "web",
            },
            json={
                "files": {
                    "mimetype_test": {
                        "file": {
                            "filename": f"hello.{extension}",
                            "content_type": mimetype,
                            "payload": "",
                        }
                    }
                }
            },
        )
        raise_for_status(resp)


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
            "query_context": ["Many football players have existed. Messi is by far the greatest."],
            "user_id": "someone@company.uk",
        },
    )
    raise_for_status(resp)
    data = io.BytesIO(resp.content)
    answer = data.read().decode("utf-8")
    print(f"Answer: {answer}")
    assert "Messi" in answer

    # Test accepting application/x-ndjson content type
    resp = requests.post(
        os.path.join(BASE_URL, f"api/v1/kb/{kbid}/predict/chat"),
        headers={
            "Content-type": "application/json",
            "Accept": "application/x-ndjson",
            "X-NUCLIADB-ROLES": "READER",
            "x-ndb-client": "web",
        },
        json={
            "question": "Who is the best football player?",
            "query_context": ["Many football players have existed. Messi is by far the greatest."],
            "user_id": "someone@company.uk",
        },
    )
    raise_for_status(resp)
    lines = [json.loads(line) for line in resp.iter_lines()]

    # Check that the answer is in the response
    text_answer = ""
    for line in lines:
        chunk = line["chunk"]
        if chunk["type"] == "text":
            text_answer += chunk["text"]
    print(f"Answer: {text_answer}")
    assert "Messi" in text_answer, f"Expected answer not found: {'\n'.join(lines)}"

    # Check that the tokens are reported
    meta = next(line["chunk"] for line in lines if line["chunk"]["type"] == "meta")
    assert meta["input_tokens"] >= 0
    assert meta["output_tokens"] >= 0


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
    raise_for_status(resp)
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
    raise_for_status(resp)
    rephrased_query = resp.json()
    # Status code 0 means success...
    assert rephrased_query.endswith("0")


def raise_for_status(resp):
    try:
        resp.raise_for_status()
    except Exception:
        print("Error response")
        print("Status code:", resp.status_code)
        print(resp.text)
        raise
