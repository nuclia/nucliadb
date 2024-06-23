from dataclasses import dataclass

import pinecone
from pinecone_text.hybrid import hybrid_convex_scale
from pinecone_text.sparse import BM25Encoder


@dataclass
class HybridQuery:
    dense: list[float]
    sparse: pinecone.SparseValues


class PineconeClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client: pinecone.Pinecone = pinecone.Pinecone(api_key=api_key)
        self.index_hosts_cache = {}

    def _get_index_host(self, index_name: str) -> str:
        if index_name not in self.index_hosts_cache:
            # TODO: for prod ready: find a way to properly cache this.
            index = self.client.describe_index(index_name)
            self.index_hosts_cache[index_name] = index.host
        return self.index_hosts_cache[index_name]

    def _get_index(self, index_name: str) -> pinecone.Index:
        host = self._get_index_host(index_name)
        return self.client.Index(host=host)

    def create_index(self, index_name: str, dimension: int) -> None:
        self.client.create_index(
            name=index_name,
            dimension=dimension,
            metric="dotproduct",
            spec=pinecone.ServerlessSpec(cloud="aws", region="us-east-1"),
        )

    def delete_index(self, index_name: str) -> None:
        self.client.delete_index(index_name, timeout=-1)

    def upsert_vectors(self, index_name: str, vectors: list[pinecone.Vector]) -> None:
        index = self._get_index(index_name)
        index.upsert(vectors=vectors)

    def query(
        self, index_name: str, hybrid_query: HybridQuery, top_k: int = 10, filter=None
    ) -> pinecone.QueryResponse:
        index = self._get_index(index_name)
        query_response = index.query(
            top_k=top_k, vector=hybrid_query.dense, sparse_vector=hybrid_query.sparse, filter=filter
        )
        return query_response

    def delete_vectors_by_id_prefix(self, index_name: str, id_prefix: str) -> None:
        index = self._get_index(index_name)
        for ids in index.list(prefix=id_prefix):
            index.delete(ids=ids)


def get_dense_vector(text: str, dimension: int = 768) -> list[float]:
    import random

    return [random.random() for _ in range(dimension)]


def get_sparse_vector(encoder: BM25Encoder, text: str) -> list[float]:
    return encoder.encode_documents(text)


def hybrid_query(text: str) -> HybridQuery:
    dense = get_dense_vector(text)
    sparse = get_sparse_vector(text)
    hybrid_dense, hybrid_sparse = hybrid_convex_scale(dense, sparse, alpha=0.8)
    return HybridQuery(dense=hybrid_dense, sparse=pinecone.SparseValues(**hybrid_sparse))


def main():
    import random
    import time

    api_key = "229b613e-22ec-4903-a5b4-37691f3f2158"
    index_dimension = 768
    index_name = f"test-index-{random.randint(0, 1000)}"

    pc = PineconeClient(api_key=api_key)

    # Create index
    pc.create_index(index_name, index_dimension)
    try:
        # Upsert vectors
        rid = "rid"
        field = "f/file"
        paragraphs = [
            "The quick brown fox jumps over the lazy dog",
            "Barack Obama was the 44th president of the United States",
        ]
        vectors = [
            pinecone.Vector(
                id=f"{rid}/{field}/0/0-10",
                values=get_dense_vector(paragraphs[0]),
                metadata={"labels": ["foo", "bar"]},
                sparse_values=pinecone.SparseValues(**get_sparse_vector(paragraphs[0])),
            ),
            pinecone.Vector(
                id=f"{rid}/{field}/1/10-100",
                values=get_dense_vector(paragraphs[1]),
                metadata={"labels": ["baz"]},
                sparse_values=pinecone.SparseValues(**get_sparse_vector(paragraphs[1])),
            ),
        ]
        pc.upsert_vectors(index_name, vectors)

        # Wait for the index to be ready
        while True:
            index = pc.client.describe_index(index_name)
            if index["status"]["ready"] and index["status"]["state"] == "Ready":
                break
            time.sleep(1)

        breakpoint()

        # Query vectors
        user_query = "What did the quick brown fox do?"
        query_resp = pc.query(index_name, hybrid_query(user_query))
        assert len(query_resp["matches"]) == 2

        query_resp = pc.query(index_name, hybrid_query(user_query), filter={"labels": "foo"})
        assert len(query_resp["matches"]) == 1

        query_resp = pc.query(
            index_name, hybrid_query(user_query), filter={"$or": [{"labels": "foo"}, {"labels": "baz"}]}
        )
        assert len(query_resp["matches"]) == 1

        query_resp = pc.query(
            index_name, hybrid_query(user_query), filter={"$or": [{"labels": "foo"}, {"labels": "baz"}]}
        )
        assert len(query_resp["matches"]) == 2

        # Delete all vectors from the resource
        pc.delete_vectors_by_id_prefix(index_name, f"{rid}/{field}")

        vectors = [
            pinecone.Vector(
                id=f"{rid}/{field}/2/30-56",
                values=get_dense_vector(paragraphs[0]),
                metadata={"labels": ["foo", "bar"]},
                sparse_values=pinecone.SparseValues(**get_sparse_vector(paragraphs[0])),
            ),
            pinecone.Vector(
                id=f"{rid}/{field}/3/56-100",
                values=get_dense_vector(paragraphs[1]),
                metadata={"labels": ["baz"]},
                sparse_values=pinecone.SparseValues(**get_sparse_vector(paragraphs[1])),
            ),
        ]
        pc.upsert_vectors(index_name, vectors)
    finally:
        breakpoint()
        # Delete index
        pc.delete_index(index_name)


if __name__ == "__main__":
    main()
