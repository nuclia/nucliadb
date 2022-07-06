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
import traceback
import uuid
from typing import AsyncIterator, Optional

from nucliadb_protos.train_pb2 import (
    GetSentenceRequest,
    GetParagraphRequest,
    GetResourcesRequest,
    GetOntologyRequest,
    GetEntitiesRequest,
    Sentence,
    Paragraph,
    Resource,
    Ontology,
    Entities,
)

from nucliadb_ingest import logger
from nucliadb_ingest.maindb.driver import TXNID
from nucliadb_ingest.orm import NODES
from nucliadb_ingest.orm.exceptions import KnowledgeBoxNotFound
from nucliadb_ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb_ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxObj
from nucliadb_ingest.orm.processor import Processor
from nucliadb_ingest.orm.resource import Resource as ResourceORM
from nucliadb_ingest.orm.shard import Shard
from nucliadb_ingest.orm.utils import get_node_klass
from nucliadb_ingest.sentry import SENTRY
from nucliadb_ingest.settings import settings
from nucliadb_ingest.utils import get_driver
from nucliadb_protos import train_pb2_grpc
from nucliadb_utils.utilities import (
    get_audit,
    get_cache,
    get_partitioning,
    get_storage,
    get_transaction,
)

if SENTRY:
    from sentry_sdk import capture_exception


class TrainServicer(train_pb2_grpc.TrainServicer):
    def __init__(self):
        self.partitions = settings.partitions

    async def initialize(self):
        storage = await get_storage()
        audit = get_audit()
        driver = await get_driver()
        cache = await get_cache()
        self.proc = Processor(driver=driver, storage=storage, audit=audit, cache=cache)
        await self.proc.initialize()

    async def finalize(self):
        await self.proc.finalize()

    async def GetSentences(self, request: GetSentenceRequest, context=None) -> AsyncIterator[Sentence, None]:  # type: ignore
        async for sentence in self.proc.kb_sentences(request):
            yield sentence
