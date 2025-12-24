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


from google.protobuf.json_format import ParseDict

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.search import logger
from nucliadb.search.predict import SendToPredictError, convert_relations
from nucliadb.search.predict_models import QueryModel
from nucliadb.search.search.chat import rpc
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.utilities import get_predict
from nucliadb_models.internal.predict import QueryInfo
from nucliadb_models.search import Image, MaxTokens
from nucliadb_protos import knowledgebox_pb2, utils_pb2


class RAOFetcher(Fetcher):
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
        super().__init__(
            kbid,
            query=query,
            user_vector=user_vector,
            vectorset=vectorset,
            rephrase=rephrase,
            rephrase_prompt=rephrase_prompt,
            generative_model=generative_model,
            query_image=query_image,
        )

        self._query_info: QueryInfo | None = None
        self._vectorset: str | None = None

    async def query_information(self) -> QueryInfo:
        if self._query_info is None:
            self._query_info = await query_information(
                kbid=self.kbid,
                query=self.query,
                semantic_model=self.user_vectorset,
                generative_model=self.generative_model,
                rephrase=self.rephrase,
                rephrase_prompt=self.rephrase_prompt,
                query_image=self.query_image,
            )
        return self._query_info

    # Retrieval

    async def get_rephrased_query(self) -> str | None:
        query_info = await self.query_information()
        return query_info.rephrased_query

    async def get_detected_entities(self) -> list[utils_pb2.RelationNode]:
        query_info = await self.query_information()
        if query_info.entities is not None:
            detected_entities = convert_relations(query_info.entities.model_dump())
        else:
            detected_entities = []
        return detected_entities

    async def get_semantic_min_score(self) -> float | None:
        query_info = await self.query_information()
        vectorset = await self.get_vectorset()
        return query_info.semantic_thresholds.get(vectorset, None)

    async def get_vectorset(self) -> str:
        if self._vectorset is None:
            if self.user_vectorset is not None:
                self._vectorset = self.user_vectorset
            else:
                # when it's not provided, we get the default from Predict API
                query_info = await self.query_information()
                if query_info.sentence is None or len(query_info.sentence.vectors) == 0:
                    logger.error(
                        "Asking for a vectorset but /query didn't return one", extra={"kbid": self.kbid}
                    )
                    raise SendToPredictError("Predict API didn't return a sentence vectorset")
                # vectors field is enforced by the data model to have at least one key
                for vectorset in query_info.sentence.vectors.keys():
                    self._vectorset = vectorset
                    break
        assert self._vectorset is not None
        return self._vectorset

    async def get_query_vector(self) -> list[float]:
        if self.user_vector is not None:
            return self.user_vector

        query_info = await self.query_information()
        if query_info.sentence is None:
            logger.error(
                "Asking for a semantic query vector but /query didn't return a sentence",
                extra={"kbid": self.kbid},
            )
            raise SendToPredictError("Predict API didn't return a sentence for semantic search")

        vectorset = await self.get_vectorset()
        if vectorset not in query_info.sentence.vectors:
            logger.error(
                "Predict is not responding with a valid query nucliadb vectorset",
                extra={
                    "kbid": self.kbid,
                    "vectorset": vectorset,
                    "predict_vectorsets": ",".join(query_info.sentence.vectors.keys()),
                },
            )
            raise SendToPredictError("Predict API didn't return the requested vectorset")

        query_vector = query_info.sentence.vectors[vectorset]
        return query_vector

    async def get_classification_labels(self) -> knowledgebox_pb2.Labels:
        labelsets = await rpc.labelsets(self.kbid)

        # TODO(decoupled-ask): remove this conversion and refactor code to use API models instead of protobuf
        kb_labels = knowledgebox_pb2.Labels()
        for labelset, labels in labelsets.labelsets.items():
            ParseDict(labels.model_dump(), kb_labels.labelset[labelset])

        return kb_labels

    # Generative

    async def get_visual_llm_enabled(self) -> bool:
        query_info = await self.query_information()
        if query_info is None:
            raise SendToPredictError("Error while using predict's query endpoint")

        return query_info.visual_llm

    async def get_max_context_tokens(self, max_tokens: MaxTokens | None) -> int:
        query_info = await self.query_information()
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


async def query_information(
    kbid: str,
    query: str,
    semantic_model: str | None,
    generative_model: str | None = None,
    rephrase: bool = False,
    rephrase_prompt: str | None = None,
    query_image: Image | None = None,
) -> QueryInfo:
    # NOTE: When moving /ask to RAO, this will need to change to whatever client/utility is used
    # to call NUA predict (internally or externally in the case of onprem).
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
