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
import contextlib
import json
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from enum import Enum, IntEnum
from typing import Any, Optional, Union

import backoff
import httpx
from fastapi import Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self

from nucliadb_protos import knowledgebox_pb2, utils_pb2
from nucliadb_telemetry import errors
from nucliadb_utils.settings import is_onprem_nucliadb, nuclia_settings

SERVICE_NAME = "nucliadb.learning_proxy"
logger = logging.getLogger(SERVICE_NAME)

WHITELISTED_HEADERS = {
    "x-nucliadb-user",
    "x-nucliadb-roles",
    "x-stf-roles",
    "x-stf-user",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-port",
}


class LearningService(Enum):
    CONFIG = "config"


class SimilarityFunction(IntEnum):
    # Keep this in sync with learning config repo
    # It's an IntEnum to match the protobuf definition
    DOT = 0
    COSINE = 1


class SemanticConfig(BaseModel):
    # Keep this in sync with learning config repo
    similarity: SimilarityFunction
    size: int
    threshold: float
    matryoshka_dims: list[int] = []

    def into_semantic_model_metadata(self) -> knowledgebox_pb2.SemanticModelMetadata:
        semantic_model = knowledgebox_pb2.SemanticModelMetadata()
        LEARNING_SIMILARITY_FUNCTION_TO_PROTO = {
            SimilarityFunction.COSINE: utils_pb2.VectorSimilarity.COSINE,
            SimilarityFunction.DOT: utils_pb2.VectorSimilarity.DOT,
        }
        semantic_model.similarity_function = LEARNING_SIMILARITY_FUNCTION_TO_PROTO[self.similarity]
        semantic_model.vector_dimension = self.size
        semantic_model.matryoshka_dimensions.extend(self.matryoshka_dims)
        return semantic_model


# Subset of learning configuration of nucliadb's interest. Look at
# learning_config models for more fields
class LearningConfiguration(BaseModel):
    semantic_model: str
    # aka similarity function
    semantic_vector_similarity: str
    # aka vector_dimension
    semantic_vector_size: Optional[int] = None
    # aka min_score
    semantic_threshold: Optional[float] = None
    # List of possible subdivisions of the matryoshka embeddings (if the model
    # supports it)
    semantic_matryoshka_dimensions: Optional[list[int]] = Field(
        default=None, alias="semantic_matryoshka_dims"
    )

    semantic_models: list[str] = Field(default_factory=list)

    # This is where the config for each semantic model (aka vectorsets) is returned
    semantic_model_configs: dict[str, SemanticConfig] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def maintain_bw_compatibility_with_single_model_configs(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if not data.get("semantic_model", None) and len(data.get("semantic_models", [])) > 0:
                data["semantic_model"] = data["semantic_models"][0]
        return data

    @model_validator(mode="after")
    def validate_matryoshka_and_vector_dimension_consistency(self) -> Self:
        vector_size = self.semantic_vector_size
        matryoshka_dimensions = self.semantic_matryoshka_dimensions or []
        if (
            len(matryoshka_dimensions) > 0
            and vector_size is not None
            and vector_size not in matryoshka_dimensions
        ):
            raise ValueError("Semantic vector size is inconsistent with matryoshka dimensions")
        return self

    def into_semantic_models_metadata(
        self,
    ) -> dict[str, knowledgebox_pb2.SemanticModelMetadata]:
        result = {}
        for model_name, config in self.semantic_model_configs.items():
            result[model_name] = config.into_semantic_model_metadata()
        return result

    def into_semantic_model_metadata(self) -> knowledgebox_pb2.SemanticModelMetadata:
        semantic_model = knowledgebox_pb2.SemanticModelMetadata()

        LEARNING_SIMILARITY_FUNCTION_TO_PROTO = {
            "cosine": utils_pb2.VectorSimilarity.COSINE,
            "dot": utils_pb2.VectorSimilarity.DOT,
        }
        semantic_model.similarity_function = LEARNING_SIMILARITY_FUNCTION_TO_PROTO[
            self.semantic_vector_similarity.lower()
        ]

        if self.semantic_vector_size is not None:
            semantic_model.vector_dimension = self.semantic_vector_size
        else:
            logger.warning("Vector dimension not set!")

        if self.semantic_matryoshka_dimensions is not None:
            semantic_model.matryoshka_dimensions.extend(self.semantic_matryoshka_dimensions)

        return semantic_model


class ProxiedLearningConfigError(Exception):
    def __init__(self, status_code: int, content: Union[str, dict[str, Any]]):
        self.status_code = status_code
        self.content = content


def raise_for_status(response: httpx.Response) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as err:
        content_type = err.response.headers.get("Content-Type", "application/json")
        if content_type == "application/json":
            content = err.response.json()
        else:
            content = err.response.text
        raise ProxiedLearningConfigError(
            status_code=err.response.status_code,
            content=content,
        )


async def get_configuration(
    kbid: str,
) -> Optional[LearningConfiguration]:
    return await learning_config_service().get_configuration(kbid)


async def set_configuration(
    kbid: str,
    config: dict[str, Any],
) -> LearningConfiguration:
    return await learning_config_service().set_configuration(kbid, config)


async def update_configuration(
    kbid: str,
    config: dict[str, Any],
) -> None:
    return await learning_config_service().update_configuration(kbid, config)


async def delete_configuration(
    kbid: str,
) -> None:
    return await learning_config_service().delete_configuration(kbid)


async def learning_config_proxy(
    request: Request,
    method: str,
    url: str,
    extra_headers: Optional[dict[str, str]] = None,
) -> Union[Response, StreamingResponse]:
    return await proxy(
        service=LearningService.CONFIG,
        request=request,
        method=method,
        url=url,
        extra_headers=extra_headers,
    )


def is_white_listed_header(header: str) -> bool:
    return header.lower() in WHITELISTED_HEADERS


@backoff.on_exception(
    backoff.expo,
    (Exception,),  # retry all unhandled http client/server errors right now
    jitter=backoff.random_jitter,
    max_tries=3,
)
async def _retriable_proxied_request(
    *,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    content: bytes,
    headers: dict[str, str],
    params: dict[str, Any],
) -> httpx.Response:
    return await client.request(
        method=method.upper(), url=url, params=params, content=content, headers=headers
    )


async def proxy(
    service: LearningService,
    request: Request,
    method: str,
    url: str,
    extra_headers: Optional[dict[str, str]] = None,
) -> Union[Response, StreamingResponse]:
    """
    Proxy the request to a learning API.

    service: LearningService. The learning service to proxy the request to.
    request: Request. The incoming request.
    method: str. The HTTP method to use.
    url: str. The URL to proxy the request to.
    extra_headers: Optional[dict[str, str]]. Extra headers to include in the proxied request.

    Returns: Response. The response from the learning API. If the response is chunked, a StreamingResponse is returned.
    """

    proxied_headers = extra_headers or {}
    proxied_headers.update(
        {k.lower(): v for k, v in request.headers.items() if is_white_listed_header(k)}
    )

    async with service_client(
        base_url=get_base_url(service=service),
        headers=get_auth_headers(),
    ) as client:
        try:
            response = await _retriable_proxied_request(
                client=client,
                method=method.upper(),
                url=url,
                params=dict(request.query_params),
                content=await request.body(),
                headers=proxied_headers,
            )
        except Exception as exc:
            errors.capture_exception(exc)
            msg = f"Unexpected error while trying to proxy the request to the learning {service.value} API."
            logger.exception(msg, exc_info=True)
            return Response(
                content=msg.encode(),
                status_code=503,
                media_type="text/plain",
            )
        if response.headers.get("Transfer-Encoding") == "chunked":
            return StreamingResponse(
                content=response.aiter_bytes(),
                status_code=response.status_code,
                headers=response.headers,
                media_type=response.headers.get("Content-Type"),
            )
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=response.headers,
            media_type=response.headers.get("Content-Type"),
        )


def get_base_url(service: LearningService) -> str:
    if is_onprem_nucliadb():
        nuclia_public_url = nuclia_settings.nuclia_public_url.format(zone=nuclia_settings.nuclia_zone)
        return f"{nuclia_public_url}/api/v1"
    learning_svc_base_url = nuclia_settings.learning_internal_svc_base_url.format(service=service.value)
    return f"{learning_svc_base_url}/api/v1/internal"


def get_auth_headers() -> dict[str, str]:
    if is_onprem_nucliadb():
        # public api: auth is done via the 'x-nuclia-nuakey' header
        return {"X-NUCLIA-NUAKEY": f"Bearer {nuclia_settings.nuclia_service_account}"}
    else:
        # internal api: auth is proxied from the request coming to NucliaDB to the learning
        # apis via the 'x-nucliadb-user' and 'x-nucliadb-roles' headers that
        # idp injects on auth lookup.
        return {}


@contextlib.asynccontextmanager
async def service_client(
    base_url: str,
    headers: dict[str, str],
) -> AsyncIterator[httpx.AsyncClient]:
    """
    Context manager for the learning client. Makes sure the client is closed after use.
    For now, a new client session is created for each request. This is to avoid having to
    save a client session in the FastAPI app state.
    """
    if nuclia_settings.dummy_learning_services:
        # This is a workaround to be able to run integration tests that start nucliadb with docker.
        # The learning APIs are not available in the docker setup, so we use a dummy client.
        client = DummyClient(base_url=base_url, headers=headers)
        logger.warning("Using dummy client. If you see this in production, something is wrong.")
    else:
        client = httpx.AsyncClient(base_url=base_url, headers=headers)  # type: ignore
    try:
        yield client
    finally:
        if client.is_closed is False:
            await client.aclose()


@contextlib.asynccontextmanager
async def learning_config_client() -> AsyncIterator[httpx.AsyncClient]:
    """
    Context manager for the learning config client.
    """
    async with service_client(
        base_url=get_base_url(LearningService.CONFIG), headers=get_auth_headers()
    ) as client:
        yield client


class DummyResponse(httpx.Response):
    def raise_for_status(self) -> httpx.Response:
        return self


class DummyClient(httpx.AsyncClient):
    def _response(self, content=None):
        if content is None:
            content = {"detail": "Dummy client is not supposed to be used"}
        return DummyResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            content=json.dumps(content).encode(),
        )

    async def get(self, *args, **kwargs: Any):
        return self._handle_request("GET", *args, **kwargs)

    async def post(self, *args: Any, **kwargs: Any):
        return self._handle_request("POST", *args, **kwargs)

    async def patch(self, *args: Any, **kwargs: Any):
        return self._handle_request("PATCH", *args, **kwargs)

    async def delete(self, *args: Any, **kwargs: Any):
        return self._handle_request("DELETE", *args, **kwargs)

    def get_config(self, *args: Any, **kwargs: Any):
        size = 768 if os.environ.get("TEST_SENTENCE_ENCODER") == "multilingual-2023-02-21" else 512
        lconfig = LearningConfiguration(
            semantic_model="multilingual",
            semantic_vector_similarity="cosine",
            semantic_vector_size=size,
            semantic_threshold=None,
            semantic_matryoshka_dims=[],
            semantic_model_configs={
                "multilingual": SemanticConfig(
                    similarity=SimilarityFunction.COSINE,
                    size=size,
                    threshold=0,
                    matryoshka_dims=[],
                )
            },
        )
        return self._response(content=lconfig.model_dump())

    def post_config(self, *args: Any, **kwargs: Any):
        # simulate post that returns the created config
        return self.get_config(*args, **kwargs)

    def patch_config(self, *args: Any, **kwargs: Any):
        # simulate patch that returns the updated config
        return self.get_config(*args, **kwargs)

    async def request(  # type: ignore
        self, method: str, url: str, params=None, content=None, headers=None, *args, **kwargs
    ) -> httpx.Response:
        return self._handle_request(method, url, params=params, content=content, headers=headers)

    def _handle_request(self, *args: Any, **kwargs: Any) -> httpx.Response:
        """
        Try to map HTTP Method + Path to methods of this class:
        e.g: GET /config/{kbid} -> get_config
        """
        http_method = args[0]
        http_url = args[1]
        method = f"{http_method.lower()}_{http_url.split('/')[0]}"
        if hasattr(self, method):
            return getattr(self, method)(*args, **kwargs)
        else:
            return self._response()


class LearningConfigService(ABC):
    @abstractmethod
    async def get_configuration(self, kbid: str) -> Optional[LearningConfiguration]: ...

    @abstractmethod
    async def set_configuration(self, kbid: str, config: dict[str, Any]) -> LearningConfiguration: ...

    @abstractmethod
    async def update_configuration(self, kbid: str, config: dict[str, Any]) -> None: ...

    @abstractmethod
    async def delete_configuration(self, kbid: str) -> None: ...


class ProxiedLearningConfig(LearningConfigService):
    async def get_configuration(self, kbid: str) -> Optional[LearningConfiguration]:
        async with self._client() as client:
            resp = await client.get(f"config/{kbid}")
            try:
                raise_for_status(resp)
            except ProxiedLearningConfigError as err:
                if err.status_code == 404:
                    return None
                raise
            return LearningConfiguration.model_validate(resp.json())

    async def set_configuration(self, kbid: str, config: dict[str, Any]) -> LearningConfiguration:
        async with self._client() as client:
            resp = await client.post(f"config/{kbid}", json=config)
            raise_for_status(resp)
            return LearningConfiguration.model_validate(resp.json())

    async def update_configuration(self, kbid: str, config: dict[str, Any]) -> None:
        async with self._client() as client:
            resp = await client.patch(f"config/{kbid}", json=config)
            raise_for_status(resp)
            return

    async def delete_configuration(self, kbid: str) -> None:
        async with self._client() as client:
            resp = await client.delete(f"config/{kbid}")
            raise_for_status(resp)

    @contextlib.asynccontextmanager
    async def _client(self) -> AsyncIterator[httpx.AsyncClient]:
        async with httpx.AsyncClient(
            base_url=get_base_url(LearningService.CONFIG),
            headers=get_auth_headers(),
        ) as client:
            yield client


_IN_MEMORY_CONFIGS: dict[str, LearningConfiguration] = {}


class InMemoryLearningConfig(LearningConfigService):
    def __init__(self):
        self.in_memory_configs = {}

    async def get_configuration(self, kbid: str) -> Optional[LearningConfiguration]:
        return _IN_MEMORY_CONFIGS.get(kbid, None)

    async def set_configuration(self, kbid: str, config: dict[str, Any]) -> LearningConfiguration:
        if not config:
            # generate a default config
            default_model = os.environ.get("TEST_SENTENCE_ENCODER", "multilingual")
            size = 768 if default_model == "multilingual-2023-02-21" else 512
            # XXX for some reason, we override the model name and set this one
            # default_model = "multilingual"
            learning_config = LearningConfiguration(
                semantic_model=default_model,
                semantic_vector_similarity="cosine",
                semantic_vector_size=size,
                semantic_threshold=None,
                semantic_matryoshka_dims=[],
                semantic_models=[default_model],
                semantic_model_configs={
                    default_model: SemanticConfig(
                        similarity=SimilarityFunction.COSINE,
                        size=size,
                        threshold=0,
                        matryoshka_dims=[],
                    )
                },
            )

        else:
            learning_config = LearningConfiguration.model_validate(config)

        _IN_MEMORY_CONFIGS[kbid] = learning_config
        return learning_config

    async def update_configuration(self, kbid: str, config: dict[str, Any]) -> None:
        if kbid not in _IN_MEMORY_CONFIGS:
            raise ValueError(f"Configuration for kbid {kbid} not found")
        learning_config = _IN_MEMORY_CONFIGS[kbid]
        learning_config = learning_config.model_copy(update=config)
        _IN_MEMORY_CONFIGS[kbid] = learning_config

    async def delete_configuration(self, kbid: str) -> None:
        _IN_MEMORY_CONFIGS.pop(kbid, None)


def learning_config_service() -> LearningConfigService:
    if nuclia_settings.dummy_learning_services:
        return InMemoryLearningConfig()
    else:
        return ProxiedLearningConfig()
