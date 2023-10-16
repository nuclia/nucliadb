import random
import os
from typing import Optional
import httpx
import asyncio
import shelve
import time

from grpc import aio  # type: ignore
from nucliadb_protos import (
    nodereader_pb2,
    nodereader_pb2_grpc,
)
from nucliadb_protos.utils_pb2 import RelationNode


PREDICT_URL = "https://europe-1.stashify.cloud/api/v1/predict"
NUA_KEY = os.environ["NUA_KEY"]


def cache_to_disk(func):
    async def new_func(*args, **kwargs):
        d = shelve.open("cache.data")
        try:
            cache_key = f"{func.__name__}::{args}::{tuple(sorted(kwargs.items()))}"
            if cache_key not in d:
                d[cache_key] = await func(*args, **kwargs)
            return d[cache_key]
        finally:
            d.close()

    return new_func


@cache_to_disk
async def convert_sentence_to_vector(sentence: str):
    client = httpx.AsyncClient(base_url=PREDICT_URL)
    resp = await client.get(
        "/sentence",
        headers={"X-STF-NUAKEY": f"Bearer {NUA_KEY}"},
        params={"text": sentence},
    )
    resp.raise_for_status()
    data = resp.json()
    return data["data"]


@cache_to_disk
async def detect_entities(sentence: str) -> list[RelationNode]:
    client = httpx.AsyncClient(base_url=PREDICT_URL)
    resp = await client.get(
        "/tokens",
        headers={"X-STF-NUAKEY": f"Bearer {NUA_KEY}"},
        params={"text": sentence},
    )
    resp.raise_for_status()
    data = resp.json()
    result = []
    for token in data["tokens"]:
        text = token["text"]
        klass = token["ner"]
        result.append(
            RelationNode(value=text, ntype=RelationNode.NodeType.ENTITY, subtype=klass)
        )
    return result


async def search(
    channel: aio.Channel,
    shard_id,
    query,
    vectorized_query,
    entities,
    min_score: float = 1.0,
    features: Optional[list[str]] = None,
    filters: Optional[list[str]] = None,
    fields: Optional[list[str]] = None,
    num_docs: int = 5,
):
    request = nodereader_pb2.SearchRequest()

    request.shard = shard_id
    request.min_score = min_score
    request.body = query
    request.result_per_page = num_docs

    if filters is not None:
        request.filter.tags.extend(filters)
    if fields is not None:
        request.fields.extend(fields)
    if features is None or "documents" in features:
        request.document = True
    if features is None or "paragraphs" in features:
        request.paragraph = True

    request.vector.extend(vectorized_query)

    if features is None or "relations" in features:
        request.relation_subgraph.entry_points.extend(entities)
        request.relation_subgraph.depth = 1

    reader = nodereader_pb2_grpc.NodeReaderStub(channel)
    start = time.time()
    res = None
    try:
        res = await reader.Search(request)
    finally:
        return res, time.time() - start


shard_ids = (
    "4508ea2f-b3e2-41ca-89b6-224172f5bb3e",
    "8888ea2f-b3e2-41ca-89b6-224172f5bb3e",
)


async def suggest(
    channel: aio.Channel,
    shard_id,
    query,
    filters: Optional[list[str]] = None,
    fields: Optional[list[str]] = None,
):
    request = nodereader_pb2.SuggestRequest()
    request.shard = shard_id
    request.body = query

    if filters is not None:
        request.filter.tags.extend(filters)
    if fields is not None:
        request.fields.extend(fields)

    reader = nodereader_pb2_grpc.NodeReaderStub(channel)
    start = time.time()
    res = None
    try:
        res = await reader.Suggest(request)
    finally:
        return res, time.time() - start


async def user_session(channel, prepared_queries, features):
    num_docs = random.randint(2, 10)
    shard_id = random.choice(shard_ids)
    query, vectorized_query, entities = random.choice(prepared_queries)

    res1, duration1 = await suggest(
        channel,
        shard_id,
        query,
    )
    res2, duration2 = await suggest(
        channel,
        shard_id,
        query,
    )
    res3, duration3 = await search(
        channel,
        shard_id,
        query,
        vectorized_query,
        entities,
        features=features,
        num_docs=num_docs,
    )

    assert res1 is not None
    assert res2 is not None
    assert res3 is not None

    # make sure we get the number of docs we asked for
    assert len(res3.document.results) == num_docs, len(res3.document.results)

    return duration1 + duration2 + duration3


if __name__ == "__main__":
    queries = [
        "what is the capital of france?",
        "what is the capital of spain?",
        "what is the capital of england?",
        "when will capitalism collapse?",
        "How big is Brazil?",
    ]

    async def burst(prepared_queries, features=None, size=500):
        channel = aio.insecure_channel("localhost:40102")
        try:
            tasks = []
            for _ in range(size):
                user = asyncio.create_task(
                    user_session(channel, prepared_queries, features)
                )
                tasks.append(user)

            return await asyncio.gather(*tasks)
        finally:
            await channel.close()

    async def test():
        prepared_queries = []
        for query in queries:
            prepared_queries.append(
                (
                    query,
                    await convert_sentence_to_vector(sentence=query),
                    await detect_entities(sentence=query),
                )
            )

        features = ["documents", "paragraphs", "relations"]
        aio.init_grpc_aio()

        burst_factor = 60
        for i in range(100):
            burst_size = (i + 1) * burst_factor
            print(f"Burst {i} of size {burst_size}")
            res = await burst(prepared_queries, features=features, size=burst_size)
            avg = sum(res) / len(res)
            assert avg < 10.0
            print(f"Average {avg} sec")
            await asyncio.sleep(1)

    asyncio.run(test())
