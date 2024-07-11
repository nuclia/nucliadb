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

CONTROL_PLANE_BASE_URL = "https://api.pinecone.io/"
INDEX_HOST_BASE_URL = "https://{index_host}/"
BASE_API_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}


class PineconeControlPlane:
    """
    Client for interacting with the Pinecone control plane API.
    """

    def __init__(self, api_key: str, http_session: httpx.AsyncClient):
        self.api_key = api_key
        self.session = http_session

    @pinecone_observer.wrap({"type": "create_index"})
    async def create_index(self, name: str, dimension: int) -> str:
        payload = {
            "name": name,
            "dimension": dimension,
            "metric": "dotproduct",
            "spec": {"serverless": {"cloud": "aws", "region": "us-east-1"}},
        }
        headers = {"Api-Key": self.api_key}
        http_response = await self.session.post("/indexes", json=payload, headers=headers)
        http_response.raise_for_status()
        response = CreateIndexResponse.model_validate(http_response.json())
        return response.host

    @pinecone_observer.wrap({"type": "delete_index"})
    async def delete_index(self, name: str) -> None:
        headers = {"Api-Key": self.api_key}
        response = await self.session.delete(f"/indexes/{name}", headers=headers)
        if response.status_code == 404:
            logger.warning("Pinecone index not found.", extra={"index_name": name})
            return
        response.raise_for_status()


class PineconeDataPlane:
    def __init__(self, api_key: str, http_session: httpx.AsyncClient):
        self.api_key = api_key
        self.session = http_session
        self._upsert_batch_size: Optional[int] = None

    @pinecone_observer.wrap({"type": "upsert"})
    async def upsert(self, vectors: list[Vector]) -> None:
        headers = {"Api-Key": self.api_key}
        payload = UpsertRequest(vectors=vectors)
        try:
            breakpoint()
            response = await self.session.post(
                "/vectors/upsert", headers=headers, json=payload.model_dump()
            )
            response.raise_for_status()
        except Exception:
            breakpoint()
            pass

    def _guess_upsert_batch_size(self, vectors: list[Vector]) -> int:
        """
        Guess a batch size so that the upsert payload does not exceed 2MB and cache it.
        Make sure the number of vectors in a batch is never bigger than 100.
        """
        if self._upsert_batch_size is None:
            # Guess and cache it.
            max_batch_size = 100
            max_upsert_payload_size = 2 * 1024 * 1024
            vector_dimension = len(vectors[0].values)
            metadata_sizes = []
            for _ in range(20):
                metadata_sizes.append(len(json.dumps(random.choice(vectors).metadata)))
            average_metadata_size = sum(metadata_sizes) / len(metadata_sizes)
            vector_size = 4 * vector_dimension + average_metadata_size
            estimated_batch_size = max_upsert_payload_size // vector_size
            self._upsert_batch_size = int(min(estimated_batch_size, max_batch_size))

        return self._upsert_batch_size

    @pinecone_observer.wrap({"type": "upsert_in_batches"})
    async def upsert_in_batches(
        self, vectors: list[Vector], batch_size: Optional[int] = None, max_parallel_batches: int = 1
    ) -> None:
        if batch_size is None:
            batch_size = self._guess_upsert_batch_size(vectors)

        tasks = []
        semaphore = asyncio.Semaphore(max_parallel_batches)

        async def _upsert_batch(batch):
            async with semaphore:
                await self.upsert(batch)

        for batch in self._batchify(vectors, size=batch_size):
            tasks.append(asyncio.create_task(_upsert_batch(batch)))
        await asyncio.gather(*tasks)

    @pinecone_observer.wrap({"type": "delete"})
    async def delete(self, ids: list[str]) -> None:
        headers = {"Api-Key": self.api_key}
        payload = {"ids": ids}
        response = await self.session.post("/vectors/delete", headers=headers, json=payload)
        response.raise_for_status()

    @pinecone_observer.wrap({"type": "list_page"})
    async def list_page(
        self, prefix: Optional[str] = None, limit: int = 100, pagination_token: Optional[str] = None
    ) -> ListResponse:
        headers = {"Api-Key": self.api_key}
        params = {"limit": str(limit)}
        if prefix is not None:
            params["prefix"] = prefix
        if pagination_token is not None:
            params["paginationToken"] = pagination_token
        response = await self.session.get(
            "/vectors/list",
            headers=headers,
            params=params,
        )
        response.raise_for_status()
        return ListResponse.model_validate(response.json())

    async def list_all(
        self, prefix: Optional[str] = None, page_size: int = 100
    ) -> AsyncGenerator[str, None]:
        pagination_token = None
        while True:
            response = await self.list_page(
                prefix=prefix, limit=page_size, pagination_token=pagination_token
            )
            for vector_id in response.vectors:
                yield vector_id.id
            if response.pagination is None:
                break
            pagination_token = response.pagination.next

    @pinecone_observer.wrap({"type": "delete_all"})
    async def delete_all(self):
        headers = {"Api-Key": self.api_key}
        payload = {"deleteAll": True, "ids": []}
        response = await self.session.post("/vectors/delete", headers=headers, json=payload)
        response.raise_for_status()

    @pinecone_observer.wrap({"type": "delete_by_id_prefix"})
    async def delete_by_id_prefix(
        self, id_prefix: str, batch_size: int = 1000, max_parallel_batches: int = 1
    ) -> None:
        semaphore = asyncio.Semaphore(max_parallel_batches)

        async def _delete_batch(batch):
            async with semaphore:
                await self.delete(batch)

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
    ) -> QueryResponse:
        headers = {"Api-Key": self.api_key}
        payload = {
            "vector": vector,
            "topK": top_k,
            "includeValues": include_values,
            "includeMetadata": include_metadata,
        }
        if filter:
            payload["filter"] = filter
        response = await self.session.post("/query", headers=headers, json=payload)
        response.raise_for_status()
        return QueryResponse.model_validate(response.json())


class PineconeSession:
    """
    Wrapper that manages the singletone session around all Pinecone http api interactions.
    """

    def __init__(self):
        self.control_plane_session = httpx.AsyncClient(
            base_url=CONTROL_PLANE_BASE_URL, headers=BASE_API_HEADERS
        )
        self.data_plane_sessions = {}

    async def finalize(self):
        if not self.control_plane_session.is_closed:
            await self.control_plane_session.aclose()
        for data_plane_session in self.data_plane_sessions.values():
            if data_plane_session.is_closed:
                continue
            await data_plane_session.aclose()
        self.data_plane_sessions.clear()

    def control_plane(self, api_key: str) -> PineconeControlPlane:
        return PineconeControlPlane(api_key=api_key, http_session=self.control_plane_session)

    def _get_data_plane_session(self, index_host: str, timeout: int = 60) -> httpx.AsyncClient:
        """
        Get a session for the given index host.
        Cache http sessions so that they are reused for the same index host.
        """
        session = self.data_plane_sessions.get(index_host, None)
        if session is not None:
            return session

        session = httpx.AsyncClient(
            base_url=INDEX_HOST_BASE_URL.format(index_host=index_host),
            headers=BASE_API_HEADERS,
            timeout=timeout,
        )
        self.data_plane_sessions[index_host] = session
        return session

    def data_plane(self, api_key: str, index_host: str, timeout: int = 60) -> PineconeDataPlane:
        return PineconeDataPlane(
            api_key=api_key, http_session=self._get_data_plane_session(index_host, timeout=timeout)
        )


async def main():
    import random
    import time

    API_KEY = "229b613e-22ec-4903-a5b4-37691f3f2158"
    INDEX_HOST = "testing-i7gbo8a.svc.aped-4627-b74a.pinecone.io"
    DIMENSION = 1024

    session = PineconeSession()
    client = session.data_plane(API_KEY, INDEX_HOST)

    # Try pushing 500k vectors. Coming from 1000 resources, each with 500 vectors.
    total_vectors = 10_000
    resources = 120
    vectors_per_resource = total_vectors // resources

    print(f"Generating {total_vectors} vectors.")
    genres = [
        "rock",
        "pop",
        "jazz",
        "classical",
        "hip-hop",
        "electronic",
        "metal",
        "country",
        "blues",
    ]
    all_vectors = {}
    for i in range(resources):
        res_vectors = all_vectors.setdefault(f"resource_{i}", [])
        for j in range(vectors_per_resource):
            genre = random.choice(genres)
            vector = Vector(
                id=f"resource_{i}_vector_{j}_genre_{genre}",
                values=[random.random() for _ in range(DIMENSION)],
                metadata={"genre": genre},
            )
            res_vectors.append(vector)
    print(f"Generated {total_vectors} vectors.")

    print("Upserting vectors.")
    start = time.time()
    for resource, vectors in all_vectors.items():
        inner_start = time.time()
        breakpoint()
        await client.upsert_in_batches(vectors, batch_size=100, max_parallel_batches=10)
        inner_end = time.time()
        print(f"Upserted {len(vectors)} vectors in {inner_end - inner_start:.2f} seconds.")
    end = time.time()
    print(f"Upserted {total_vectors} vectors in {end - start:.2f} seconds.")

    print("Deleting vectors.")
    start = time.time()
    for i in range(resources):
        prefix = f"resource_{i}"
        await client.delete_by_id_prefix(prefix, batch_size=1000, max_parallel_batches=10)
    end = time.time()
    print(f"Deleted {total_vectors} vectors in {end - start:.2f} seconds.")


if __name__ == "__main__":
    asyncio.run(main())
