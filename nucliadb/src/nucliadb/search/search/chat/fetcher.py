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

import logging
from typing import Optional

from nucliadb.search.predict import SendToPredictError, convert_relations
from nucliadb.search.predict_models import QueryModel
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.utilities import get_predict
from nucliadb_models.internal.predict import QueryInfo
from nucliadb_models.search import Image
from nucliadb_protos import utils_pb2

logger = logging.getLogger(__name__)


class RAOFetcher(Fetcher):
    def __init__(
        self,
        kbid: str,
        *,
        query: str,
        user_vector: Optional[list[float]],
        vectorset: Optional[str],
        rephrase: bool,
        rephrase_prompt: Optional[str],
        generative_model: Optional[str],
        query_image: Optional[Image],
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

        self._query_info: Optional[QueryInfo] = None
        self._vectorset: Optional[str] = None

    async def query_information(self) -> QueryInfo:
        if self._query_info is None:
            self._query_info = await query_information(
                kbid=self.kbid,
                query=self.query,
                semantic_model=None,
                generative_model=self.generative_model,
                rephrase=self.rephrase,
                rephrase_prompt=self.rephrase_prompt,
                query_image=self.query_image,
            )
        return self._query_info

    async def get_rephrased_query(self) -> Optional[str]:
        query_info = await self.query_information()
        return query_info.rephrased_query

    async def get_detected_entities(self) -> list[utils_pb2.RelationNode]:
        query_info = await self.query_information()
        if query_info.entities is not None:
            detected_entities = convert_relations(query_info.entities.model_dump())
        else:
            detected_entities = []
        return detected_entities

    async def get_semantic_min_score(self) -> Optional[float]:
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


async def query_information(
    kbid: str,
    query: str,
    semantic_model: Optional[str],
    generative_model: Optional[str] = None,
    rephrase: bool = False,
    rephrase_prompt: Optional[str] = None,
    query_image: Optional[Image] = None,
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
