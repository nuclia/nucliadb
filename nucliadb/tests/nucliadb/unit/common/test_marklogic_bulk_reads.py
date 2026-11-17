import json

import httpx
import pytest

from nucliadb.common.marklogic.client import DocumentManager, Transaction
from nucliadb.common.marklogic.exceptions import DatabaseDoesNotExist


async def test_bulk_read_posts_uris_and_returns_documents():
    uris = [f"/resources/{index}/fields/t/body/processed.json" for index in range(200)]
    params = {"database": "test-kb"}

    def handler(request):
        assert request.method == "POST"
        assert request.url.path == "/v1/search"
        assert "uri" not in request.url.params
        assert request.url.params["database"] == "test-kb"
        assert request.url.params["txid"] == "test-tx"
        assert request.url.params["view"] == "none"
        assert request.url.params["pageLength"] == "200"
        assert request.headers["accept"] == "multipart/mixed"
        assert json.loads(request.content) == {"query": {"document-query": {"uri": uris}}}
        body = (
            b"".join(
                (
                    "--test\r\nContent-Type: application/json\r\n"
                    f'Content-Disposition: attachment; filename="{uri}"; category=content\r\n'
                    f'\r\n{{"index": {index}}}\r\n'
                ).encode()
                for index, uri in enumerate(uris)
            )
            + b"--test--\r\n"
        )
        return httpx.Response(
            200, content=body, headers={"content-type": "multipart/mixed; boundary=test"}
        )

    async with httpx.AsyncClient(
        base_url="http://marklogic", transport=httpx.MockTransport(handler)
    ) as http:
        result = await DocumentManager(http).read(uris, Transaction("test-tx", http), params)
    assert [document.uri for document in result] == uris
    assert [document.content for document in result] == [{"index": index} for index in range(200)]
    assert params == {"database": "test-kb"}


@pytest.mark.parametrize("uris", ["/missing.json", ["/missing.json"], ["/a.json", "/b.json"]])
async def test_missing_documents(uris):
    def handler(request):
        expected = "POST" if isinstance(uris, list) and len(uris) > 1 else "GET"
        assert request.method == expected
        return httpx.Response(404)

    async with httpx.AsyncClient(
        base_url="http://marklogic", transport=httpx.MockTransport(handler)
    ) as http:
        assert await DocumentManager(http).read(uris) == []


async def test_bulk_read_missing_database_raises():
    def handler(request):
        return httpx.Response(404, json={"errorResponse": {"messageCode": "XDMP-NOSUCHDB"}})

    async with httpx.AsyncClient(
        base_url="http://marklogic", transport=httpx.MockTransport(handler)
    ) as http:
        with pytest.raises(DatabaseDoesNotExist):
            await DocumentManager(http).read(["/a.json", "/b.json"])


async def test_empty_bulk_read_sends_no_request():
    def handler(request):
        raise AssertionError("Empty reads must not send a request")

    async with httpx.AsyncClient(
        base_url="http://marklogic", transport=httpx.MockTransport(handler)
    ) as http:
        assert await DocumentManager(http).read([]) == []
