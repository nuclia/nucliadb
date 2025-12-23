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
from typing import TypeVar

from async_lru import alru_cache
from typing_extensions import TypeIs

from nucliadb.common import datamanagers
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.maindb.utils import get_driver
from nucliadb.search import logger
from nucliadb.search.predict import SendToPredictError, convert_relations
from nucliadb.search.predict_models import QueryModel
from nucliadb.search.search.metrics import query_parse_dependency_observer
from nucliadb.search.utilities import get_predict
from nucliadb_models.internal.predict import QueryInfo
from nucliadb_models.search import Image, MaxTokens
from nucliadb_protos import knowledgebox_pb2, utils_pb2


# We use a class as cache miss marker to allow None values in the cache and to
# make mypy happy with typing
class NotCached:
    pass


not_cached = NotCached()


T = TypeVar("T")


def is_cached(field: T | NotCached) -> TypeIs[T]:
    return not isinstance(field, NotCached)


class FetcherCache:
    predict_query_info: QueryInfo | None | NotCached = not_cached

    # semantic search
    vectorset: str | NotCached = not_cached

    labels: knowledgebox_pb2.Labels | NotCached = not_cached

    synonyms: knowledgebox_pb2.Synonyms | None | NotCached = not_cached

    deleted_entity_groups: list[str] | NotCached = not_cached
    detected_entities: list[utils_pb2.RelationNode] | NotCached = not_cached


class Fetcher:
    """Queries are getting more and more complex and different phases of the
    query depend on different data, not only from the user but from other parts
    of the system.

    This class is an encapsulation of data gathering across different parts of
    the system. Given the user query input, it aims to be as efficient as
    possible removing redundant expensive calls to other parts of the system. An
    instance of a fetcher caches it's results and it's thought to be used in the
    context of a single request. DO NOT use this as a global object!

    """

    def __init__(
        self,
        kbid: str,
        *,
        query: str,
        user_vector: list[float] | None,
        vectorset: str | None,
        rephrase: bool,
        rephrase_prompt: str | None,
        generative_model: str | None,
        query_image: Image | None,
    ):
        self.kbid = kbid
        self.query = query
        self.user_vector = user_vector
        self.user_vectorset = vectorset
        self.user_vectorset_validated = False
        self.rephrase = rephrase
        self.rephrase_prompt = rephrase_prompt
        self.generative_model = generative_model
        self.query_image = query_image

        self.cache = FetcherCache()
        self.locks: dict[str, asyncio.Lock] = {}

    # Semantic search

    async def get_matryoshka_dimension(self) -> int | None:
        vectorset = await self.get_vectorset()
        return await self.get_matryoshka_dimension_cached(self.kbid, vectorset)

    async def get_user_vectorset(self) -> str | None:
        """Returns the user's requested vectorset and validates if it does exist
        in the KB.

        """
        async with self.locks.setdefault("user_vectorset", asyncio.Lock()):
            if not self.user_vectorset_validated:
                if self.user_vectorset is not None:
                    await self.validate_vectorset(self.kbid, self.user_vectorset)
            self.user_vectorset_validated = True
            return self.user_vectorset

    async def get_vectorset(self) -> str:
        """Get the vectorset to be used in the search. If not specified, by the
        user, Predict API or the own uses KB will provide a default.

        """
        async with self.locks.setdefault("vectorset", asyncio.Lock()):
            if is_cached(self.cache.vectorset):
                return self.cache.vectorset

            user_vectorset = await self.get_user_vectorset()
            if user_vectorset:
                # user explicitly asked for a vectorset
                self.cache.vectorset = user_vectorset
                return user_vectorset

            # when it's not provided, we get the default from Predict API
            query_info = await self._predict_query_endpoint()
            if query_info is None:
                vectorset = None
            else:
                if query_info.sentence is None:
                    logger.error(
                        "Asking for a vectorset but /query didn't return one", extra={"kbid": self.kbid}
                    )
                    vectorset = None
                else:
                    # vectors field is enforced by the data model to have at least one key
                    for vectorset in query_info.sentence.vectors.keys():
                        vectorset = vectorset
                        break

            if vectorset is None:
                # in case predict don't answer which vectorset to use, fallback to
                # the first vectorset of the KB
                async with datamanagers.with_ro_transaction() as txn:
                    async for vectorset, _ in datamanagers.vectorsets.iter(txn, kbid=self.kbid):
                        break
                assert vectorset is not None, "All KBs must have at least one vectorset in maindb"

            self.cache.vectorset = vectorset
            return vectorset

    async def get_query_vector(self) -> list[float] | None:
        if self.user_vector is not None:
            query_vector = self.user_vector
        else:
            query_info = await self._predict_query_endpoint()
            if query_info is None or query_info.sentence is None:
                return None

            vectorset = await self.get_vectorset()
            if vectorset not in query_info.sentence.vectors:
                logger.warning(
                    "Predict is not responding with a valid query nucliadb vectorset",
                    extra={
                        "kbid": self.kbid,
                        "vectorset": vectorset,
                        "predict_vectorsets": ",".join(query_info.sentence.vectors.keys()),
                    },
                )
                return None

            query_vector = query_info.sentence.vectors[vectorset]

        matryoshka_dimension = await self.get_matryoshka_dimension()
        if matryoshka_dimension is not None:
            if self.user_vector is not None and len(query_vector) < matryoshka_dimension:
                raise InvalidQueryError(
                    "vector",
                    f"Invalid vector length, please check valid embedding size for {vectorset} model",
                )

            # KB using a matryoshka embeddings model, cut the query vector
            # accordingly
            query_vector = query_vector[:matryoshka_dimension]

        return query_vector

    async def get_rephrased_query(self) -> str | None:
        query_info = await self._predict_query_endpoint()
        if query_info is None:
            return None
        return query_info.rephrased_query

    def get_cached_rephrased_query(self) -> str | None:
        if not is_cached(self.cache.predict_query_info):
            return None
        if self.cache.predict_query_info is None:
            return None
        return self.cache.predict_query_info.rephrased_query

    async def get_semantic_min_score(self) -> float | None:
        query_info = await self._predict_query_endpoint()
        if query_info is None:
            return None

        vectorset = await self.get_vectorset()
        min_score = query_info.semantic_thresholds.get(vectorset, None)
        return min_score

    # Labels

    async def get_classification_labels(self) -> knowledgebox_pb2.Labels:
        async with self.locks.setdefault("classification_labels", asyncio.Lock()):
            if is_cached(self.cache.labels):
                return self.cache.labels

            labels = await get_classification_labels(self.kbid)
            self.cache.labels = labels
            return labels

    # Entities

    async def get_detected_entities(self) -> list[utils_pb2.RelationNode]:
        async with self.locks.setdefault("detected_entities", asyncio.Lock()):
            if is_cached(self.cache.detected_entities):
                return self.cache.detected_entities

            # Optimization to avoid calling predict twice
            if is_cached(self.cache.predict_query_info):
                # /query supersets detect entities, so we already have them
                query_info = self.cache.predict_query_info
                if query_info is not None and query_info.entities is not None:
                    detected_entities = convert_relations(query_info.entities.model_dump())
                else:
                    detected_entities = []
            else:
                # No call to /query has been done, we'll use detect entities
                # endpoint instead (as it's faster)
                detected_entities = await self._predict_detect_entities()

            self.cache.detected_entities = detected_entities
            return detected_entities

    # Synonyms

    async def get_synonyms(self) -> knowledgebox_pb2.Synonyms | None:
        async with self.locks.setdefault("synonyms", asyncio.Lock()):
            if is_cached(self.cache.synonyms):
                return self.cache.synonyms

            synonyms = await get_kb_synonyms(self.kbid)
            self.cache.synonyms = synonyms
            return synonyms

    # Generative

    async def get_visual_llm_enabled(self) -> bool:
        query_info = await self._predict_query_endpoint()
        if query_info is None:
            raise SendToPredictError("Error while using predict's query endpoint")

        return query_info.visual_llm

    async def get_max_context_tokens(self, max_tokens: MaxTokens | None) -> int:
        query_info = await self._predict_query_endpoint()
        if query_info is None:
            raise SendToPredictError("Error while using predict's query endpoint")

        model_max = query_info.max_context
        if max_tokens is not None and max_tokens.context is not None:
            if max_tokens.context > model_max:
                raise InvalidQueryError(
                    "max_tokens.context",
                    f"Max context tokens is higher than the model's limit of {model_max}",
                )
            return max_tokens.context
        return model_max

    def get_max_answer_tokens(self, max_tokens: MaxTokens | None) -> int | None:
        if max_tokens is not None and max_tokens.answer is not None:
            return max_tokens.answer
        return None

    # Predict API

    async def _predict_query_endpoint(self) -> QueryInfo | None:
        async with self.locks.setdefault("predict_query_endpoint", asyncio.Lock()):
            if is_cached(self.cache.predict_query_info):
                return self.cache.predict_query_info

            # we can't call get_vectorset, as it would do a recirsive loop between
            # functions, so we'll manually parse it
            vectorset = await self.get_user_vectorset()
            try:
                query_info = await query_information(
                    self.kbid,
                    self.query,
                    vectorset,
                    self.generative_model,
                    self.rephrase,
                    self.rephrase_prompt,
                    self.query_image,
                )
            except (SendToPredictError, TimeoutError):
                query_info = None

            self.cache.predict_query_info = query_info
            return query_info

    async def _predict_detect_entities(self) -> list[utils_pb2.RelationNode]:
        try:
            detected_entities = await detect_entities(self.kbid, self.query)
        except (SendToPredictError, TimeoutError) as ex:
            logger.warning(f"Errors on Predict API detecting entities: {ex}", extra={"kbid": self.kbid})
            detected_entities = []

        return detected_entities

    async def validate_vectorset(self, kbid: str, vectorset: str):
        async with datamanagers.with_ro_transaction() as txn:
            if not await datamanagers.vectorsets.exists(txn, kbid=kbid, vectorset_id=vectorset):
                raise InvalidQueryError(
                    "vectorset", f"Vectorset {vectorset} doesn't exist in your Knowledge Box"
                )

    @alru_cache(maxsize=10)
    async def get_matryoshka_dimension_cached(self, kbid: str, vectorset: str) -> int | None:
        # This can be safely cached as the matryoshka dimension is not expected to change
        return await get_matryoshka_dimension(kbid, vectorset)


@query_parse_dependency_observer.wrap({"type": "query_information"})
async def query_information(
    kbid: str,
    query: str,
    semantic_model: str | None,
    generative_model: str | None = None,
    rephrase: bool = False,
    rephrase_prompt: str | None = None,
    query_image: Image | None = None,
) -> QueryInfo:
    predict = get_predict()
    item = QueryModel(
        text=query,
        semantic_models=[semantic_model] if semantic_model else None,
        generative_model=generative_model,
        rephrase=rephrase,
        rephrase_prompt=rephrase_prompt,
        query_image=query_image,
    )
    return await predict.query(kbid, item)


@query_parse_dependency_observer.wrap({"type": "detect_entities"})
async def detect_entities(kbid: str, query: str) -> list[utils_pb2.RelationNode]:
    predict = get_predict()
    return await predict.detect_entities(kbid, query)


@query_parse_dependency_observer.wrap({"type": "matryoshka_dimension"})
async def get_matryoshka_dimension(kbid: str, vectorset: str | None) -> int | None:
    async with get_driver().ro_transaction() as txn:
        matryoshka_dimension = None
        if not vectorset:
            # XXX this should be migrated once we remove the "default" vectorset
            # concept
            matryoshka_dimension = await datamanagers.kb.get_matryoshka_vector_dimension(txn, kbid=kbid)
        else:
            vectorset_config = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset)
            if vectorset_config is not None and vectorset_config.vectorset_index_config.vector_dimension:
                matryoshka_dimension = vectorset_config.vectorset_index_config.vector_dimension

        return matryoshka_dimension


@query_parse_dependency_observer.wrap({"type": "classification_labels"})
async def get_classification_labels(kbid: str) -> knowledgebox_pb2.Labels:
    async with get_driver().ro_transaction() as txn:
        return await datamanagers.labels.get_labels(txn, kbid=kbid)


@query_parse_dependency_observer.wrap({"type": "synonyms"})
async def get_kb_synonyms(kbid: str) -> knowledgebox_pb2.Synonyms | None:
    async with get_driver().ro_transaction() as txn:
        return await datamanagers.synonyms.get(txn, kbid=kbid)
