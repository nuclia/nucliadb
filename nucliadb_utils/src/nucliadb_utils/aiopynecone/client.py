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
#
import asyncio
import json
import logging
import random
from typing import Any, AsyncGenerator, Optional

import httpx

from nucliadb_telemetry.metrics import Observer
from nucliadb_utils.aiopynecone.models import (
    CreateIndexResponse,
    ListResponse,
    QueryResponse,
    UpsertRequest,
    Vector,
)

logger = logging.getLogger(__name__)

pinecone_observer = Observer(
    "pinecone_client",
    labels={"type": ""},
)

DEFAULT_TIMEOUT = 30
CONTROL_PLANE_BASE_URL = "https://api.pinecone.io/"
INDEX_HOST_BASE_URL = "https://{index_host}/"
BASE_API_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}
MB = 1024 * 1024
MAX_UPSERT_PAYLOAD_SIZE = 2 * MB
MAX_DELETE_BATCH_SIZE = 1000


class PineconeAPIError(Exception):
    def __init__(
        self,
        http_status_code: int,
        code: Optional[str] = None,
        message: Optional[str] = None,
        details: Optional[Any] = None,
    ):
        self.http_status_code = http_status_code
        self.code = code or ""
        self.message = message
        self.details = details
        exc_message = (
            f"[{http_status_code}] message=\"{message or ""}\" code={code or ""} details={details}"
        )
        super().__init__(exc_message)


class ControlPlane:
    """
    Client for interacting with the Pinecone control plane API.
    https://docs.pinecone.io/reference/api/control-plane
    """

    def __init__(self, api_key: str, http_session: httpx.AsyncClient):
        self.api_key = api_key
        self.http_session = http_session

    @pinecone_observer.wrap({"type": "create_index"})
    async def create_index(self, name: str, dimension: int) -> str:
        payload = {
            "name": name,
            "dimension": dimension,
            "metric": "dotproduct",
            "spec": {"serverless": {"cloud": "aws", "region": "us-east-1"}},
        }
        headers = {"Api-Key": self.api_key}
        http_response = await self.http_session.post("/indexes", json=payload, headers=headers)
        raise_for_status(http_response)
        response = CreateIndexResponse.model_validate(http_response.json())
        return response.host

    @pinecone_observer.wrap({"type": "delete_index"})
    async def delete_index(self, name: str) -> None:
        headers = {"Api-Key": self.api_key}
        response = await self.http_session.delete(f"/indexes/{name}", headers=headers)
        if response.status_code == 404:
            logger.warning("Pinecone index not found.", extra={"index_name": name})
            return
        raise_for_status(response)


class DataPlane:
    """
    Client for interacting with the Pinecone data plane API, hosted by an index host.
    https://docs.pinecone.io/reference/api/data-plane
    """

    def __init__(
        self, api_key: str, index_host_session: httpx.AsyncClient, timeout: Optional[float] = None
    ):
        """
        Params:
        - `api_key`: The Pinecone API key.
        - `index_host_session`: The http session for the index host.
        - `timeout`: The default timeout for all requests. If not set, the default timeout from httpx.AsyncClient is used.
        """
        self.api_key = api_key
        self.http_session = index_host_session
        self.client_timeout = timeout
        self._upsert_batch_size: Optional[int] = None

    def _get_request_timeout(self, timeout: Optional[float] = None) -> Optional[float]:
        return timeout or self.client_timeout

    @pinecone_observer.wrap({"type": "upsert"})
    async def upsert(self, vectors: list[Vector], timeout: Optional[float] = None) -> None:
        """
        Upsert vectors into the index.
        Params:
        - `vectors`: The vectors to upsert.
        - `timeout`: to control the request timeout. If not set, the default timeout is used.
        """
        headers = {"Api-Key": self.api_key}
        payload = UpsertRequest(vectors=vectors)
        post_kwargs: dict[str, Any] = {
            "headers": headers,
            "json": payload.model_dump(),
        }
        request_timeout = self._get_request_timeout(timeout)
        if request_timeout is not None:
            post_kwargs["timeout"] = timeout
        response = await self.http_session.post("/vectors/upsert", **post_kwargs)
        raise_for_status(response)

    def _estimate_upsert_batch_size(self, vectors: list[Vector]) -> int:
        """
        Estimate a batch size so that the upsert payload does not exceed the hard limit.
        https://docs.pinecone.io/reference/quotas-and-limits#hard-limits
        """
        if self._upsert_batch_size is not None:
            # Return the cached value.
            return self._upsert_batch_size
        # Take the dimension of the first vector as the vector dimension.
        # Assumes all vectors have the same dimension.
        vector_dimension = len(vectors[0].values)
        # Estimate the metadata size by taking the average of 20 random vectors.
        metadata_sizes = []
        for _ in range(20):
            metadata_sizes.append(len(json.dumps(random.choice(vectors).metadata)))
        average_metadata_size = sum(metadata_sizes) / len(metadata_sizes)
        # Estimate the size of the vector payload. 4 bytes per float.
        vector_size = 4 * vector_dimension + average_metadata_size
        # Cache the value.
        self._upsert_batch_size = max(int(MAX_UPSERT_PAYLOAD_SIZE // vector_size), 1)
        return self._upsert_batch_size

    @pinecone_observer.wrap({"type": "upsert_in_batches"})
    async def upsert_in_batches(
        self,
        vectors: list[Vector],
        batch_size: Optional[int] = None,
        max_parallel_batches: int = 1,
        batch_timeout: Optional[float] = None,
    ) -> None:
        """
        Upsert vectors in batches.
        Params:
        - `vectors`: The vectors to upsert.
        - `batch_size`: to control the number of vectors in each batch.
        - `max_parallel_batches`: to control the number of batches sent concurrently.
        - `batch_timeout`: to control the request timeout for each batch.
        """
        if batch_size is None:
            batch_size = self._estimate_upsert_batch_size(vectors)

        semaphore = asyncio.Semaphore(max_parallel_batches)

        async def _upsert_batch(batch):
            async with semaphore:
                await self.upsert(vectors=batch, timeout=batch_timeout)

        tasks = []
        for batch in self._batchify(vectors, size=batch_size):
            tasks.append(asyncio.create_task(_upsert_batch(batch)))

        await asyncio.gather(*tasks)

    @pinecone_observer.wrap({"type": "delete"})
    async def delete(self, ids: list[str], timeout: Optional[float] = None) -> None:
        """
        Delete vectors by their ids.
        Maximum number of ids in a single request is 1000.
        """
        headers = {"Api-Key": self.api_key}
        payload = {"ids": ids}
        post_kwargs: dict[str, Any] = {
            "headers": headers,
            "json": payload,
        }
        request_timeout = self._get_request_timeout(timeout)
        if request_timeout is not None:
            post_kwargs["timeout"] = timeout
        response = await self.http_session.post("/vectors/delete", **post_kwargs)
        raise_for_status(response)

    @pinecone_observer.wrap({"type": "list_page"})
    async def list_page(
        self,
        prefix: Optional[str] = None,
        limit: int = 100,
        pagination_token: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> ListResponse:
        """
        List vectors in a paginated manner.
        Params:
        - `prefix`: to filter vectors by their id prefix.
        - `limit`: to control the number of vectors fetched in each page.
        - `pagination_token`: to fetch the next page. The token is provided in the response
           if there are more pages to fetch.
        """
        headers = {"Api-Key": self.api_key}
        params = {"limit": str(limit)}
        if prefix is not None:
            params["prefix"] = prefix
        if pagination_token is not None:
            params["paginationToken"] = pagination_token

        post_kwargs: dict[str, Any] = {
            "headers": headers,
            "params": params,
        }
        request_timeout = self._get_request_timeout(timeout)
        if request_timeout is not None:
            post_kwargs["timeout"] = timeout
        response = await self.http_session.get(
            "/vectors/list",
            **post_kwargs,
        )
        raise_for_status(response)
        return ListResponse.model_validate(response.json())

    async def list_all(
        self, prefix: Optional[str] = None, page_size: int = 100, page_timeout: Optional[float] = None
    ) -> AsyncGenerator[str, None]:
        """
        Iterate over all vector ids from the index in a paginated manner.
        Params:
        - `prefix`: to filter vectors by their id prefix.
        - `page_size`: to control the number of vectors fetched in each page.
        """
        pagination_token = None
        while True:
            response = await self.list_page(
                prefix=prefix, limit=page_size, pagination_token=pagination_token, timeout=page_timeout
            )
            for vector_id in response.vectors:
                yield vector_id.id
            if response.pagination is None:
                break
            pagination_token = response.pagination.next

    @pinecone_observer.wrap({"type": "delete_all"})
    async def delete_all(self, timeout: Optional[float] = None):
        """
        Delete all vectors in the index.
        """
        headers = {"Api-Key": self.api_key}
        payload = {"deleteAll": True, "ids": [], "namespace": ""}
        post_kwargs: dict[str, Any] = {
            "headers": headers,
            "json": payload,
        }
        request_timeout = self._get_request_timeout(timeout)
        if request_timeout is not None:
            post_kwargs["timeout"] = timeout
        response = await self.http_session.post("/vectors/delete", **post_kwargs)
        try:
            raise_for_status(response)
        except PineconeAPIError as err:
            if err.http_status_code == 404 and err.code == 5:
                # Namespace not found. No vectors to delete.
                return
            raise

    @pinecone_observer.wrap({"type": "delete_by_id_prefix"})
    async def delete_by_id_prefix(
        self,
        id_prefix: str,
        batch_size: int = MAX_DELETE_BATCH_SIZE,
        max_parallel_batches: int = 1,
        batch_timeout: Optional[float] = None,
    ) -> None:
        """
        Delete vectors by their id prefix. It lists all vectors with the given prefix and deletes them in batches.
        Params:
        - `id_prefix`: to filter vectors by their id prefix.
        - `batch_size`: to control the number of vectors deleted in each batch. Maximum is 1000.
        - `max_parallel_batches`: to control the number of batches sent concurrently.
        """
        if batch_size > MAX_DELETE_BATCH_SIZE:
            logger.warning(f"Batch size {batch_size} is too large. Limiting to {MAX_DELETE_BATCH_SIZE}.")
            batch_size = MAX_DELETE_BATCH_SIZE

        semaphore = asyncio.Semaphore(max_parallel_batches)

        async def _delete_batch(batch):
            async with semaphore:
                await self.delete(ids=batch, timeout=batch_timeout)

        tasks = []
        async for batch in self._async_batchify(self.list_all(prefix=id_prefix), size=batch_size):
            tasks.append(asyncio.create_task(_delete_batch(batch)))

        await asyncio.gather(*tasks)

    def _batchify(self, iterable, size: int):
        """
        Split an iterable into chunks of size.
        """
        for i in range(0, len(iterable), size):
            yield iterable[i : i + size]

    async def _async_batchify(self, async_iterable, size: int):
        """
        Split an async iterable into chunks of size.
        """
        batch = []
        async for item in async_iterable:
            batch.append(item)
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch

    @pinecone_observer.wrap({"type": "query"})
    async def query(
        self,
        vector: list[float],
        top_k: int = 20,
        include_values: bool = False,
        include_metadata: bool = False,
        filter: Optional[dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> QueryResponse:
        """
        Query the index for similar vectors to the given vector.
        Params:
        - `vector`: The query vector.
        - `top_k`: to control the number of similar vectors to return.
        - `include_values`: to include the vector values in the response.
        - `include_metadata`: to include the vector metadata in the response.
        - `filter`: to filter the vectors by their metadata. See:
           https://docs.pinecone.io/guides/data/filter-with-metadata#metadata-query-language
        """
        headers = {"Api-Key": self.api_key}
        payload = {
            "vector": vector,
            "topK": top_k,
            "includeValues": include_values,
            "includeMetadata": include_metadata,
        }
        if filter:
            payload["filter"] = filter
        post_kwargs: dict[str, Any] = {
            "headers": headers,
            "json": payload,
        }
        request_timeout = self._get_request_timeout(timeout)
        if request_timeout is not None:
            post_kwargs["timeout"] = timeout
        response = await self.http_session.post("/query", **post_kwargs)
        raise_for_status(response)
        return QueryResponse.model_validate(response.json())


class PineconeSession:
    """
    Wrapper class that manages the sessions around all Pinecone http api interactions.
    Holds a single control plane session and multiple data plane sessions, one for each index host.
    """

    def __init__(self):
        self.control_plane_session = httpx.AsyncClient(
            base_url=CONTROL_PLANE_BASE_URL, headers=BASE_API_HEADERS, timeout=DEFAULT_TIMEOUT
        )
        self.index_host_sessions = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.finalize()

    async def finalize(self):
        if not self.control_plane_session.is_closed:
            await self.control_plane_session.aclose()
        for session in self.index_host_sessions.values():
            if session.is_closed:
                continue
            await session.aclose()
        self.index_host_sessions.clear()

    def control_plane(self, api_key: str) -> ControlPlane:
        return ControlPlane(api_key=api_key, http_session=self.control_plane_session)

    def _get_index_host_session(self, index_host: str) -> httpx.AsyncClient:
        """
        Get a session for the given index host.
        Cache http sessions so that they are reused for the same index host.
        """
        session = self.index_host_sessions.get(index_host, None)
        if session is not None:
            return session

        session = httpx.AsyncClient(
            base_url=INDEX_HOST_BASE_URL.format(index_host=index_host),
            headers=BASE_API_HEADERS,
            timeout=DEFAULT_TIMEOUT,
        )
        self.index_host_sessions[index_host] = session
        return session

    def data_plane(self, api_key: str, index_host: str, timeout: Optional[float] = None) -> DataPlane:
        index_host_session = self._get_index_host_session(index_host)
        return DataPlane(api_key=api_key, index_host_session=index_host_session, timeout=timeout)


def raise_for_status(response: httpx.Response):
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        code = None
        message = None
        details = None
        try:
            resp_json = response.json()
            code = resp_json.get("code")
            message = resp_json.get("message")
            details = resp_json.get("details")
        except Exception:
            message = response.text
        raise PineconeAPIError(
            http_status_code=response.status_code,
            code=code,
            message=message,
            details=details,
        )
