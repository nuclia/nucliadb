# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from enum import Enum
from typing import Union

from pydantic import BaseModel


class ExternalIndexProviderType(str, Enum):
    """
    Enum for the different external index providers.
    For now only Pinecone is supported, but we may add more in the future.
    """

    PINECONE = "pinecone"


class ExternalIndexProviderBase(BaseModel):
    type: ExternalIndexProviderType


class PineconeServerlessCloud(str, Enum):
    """
    List of cloud providers supported by Pinecone serverless vector database.
    """

    AWS_US_EAST_1 = "aws_us_east_1"
    AWS_US_WEST_2 = "aws_us_west_2"
    AWS_EU_WEST_1 = "aws_eu_west_1"
    GCP_US_CENTRAL1 = "gcp_us_central1"
    AZURE_EASTUS2 = "azure_eastus2"


class PineconeIndexProvider(ExternalIndexProviderBase):
    type: ExternalIndexProviderType = ExternalIndexProviderType.PINECONE
    api_key: str
    serverless_cloud: PineconeServerlessCloud


ExternalIndexProvider = Union[PineconeIndexProvider,]
