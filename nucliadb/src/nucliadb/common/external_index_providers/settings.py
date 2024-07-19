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
from pydantic import Field
from pydantic_settings import BaseSettings


class ExternalIndexProvidersSettings(BaseSettings):
    pinecone_upsert_parallelism: int = Field(
        default=3,
        title="Pinecone upsert parallelism",
        description="Number of parallel upserts to Pinecone on each set resource operation",
    )
    pinecone_delete_parallelism: int = Field(
        default=2,
        title="Pinecone delete parallelism",
        description="Number of parallel deletes to Pinecone on each delete resource operation",
    )
    pinecone_upsert_timeout: float = Field(
        default=10.0,
        title="Pinecone upsert timeout",
        description="Timeout in seconds for each upsert operation to Pinecone",
    )
    pinecone_delete_timeout: float = Field(
        default=10.0,
        title="Pinecone delete timeout",
        description="Timeout in seconds for each delete operation to Pinecone",
    )
    pinecone_query_timeout: float = Field(
        default=10.0,
        title="Pinecone query timeout",
        description="Timeout in seconds for each query operation to Pinecone",
    )


settings = ExternalIndexProvidersSettings()
