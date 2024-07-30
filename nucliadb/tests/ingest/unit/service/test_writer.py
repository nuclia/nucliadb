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
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.external_index_providers.exceptions import ExternalIndexCreationError
from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb.ingest.service.writer import WriterServicer
from nucliadb_protos import writer_pb2
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_protos.resources_pb2 import FieldText
from nucliadb_protos.utils_pb2 import ReleaseChannel, VectorSimilarity


class TestWriterServicer:
    @pytest.fixture
    def writer(self, hosted_nucliadb):
        servicer = WriterServicer()
        servicer.driver = AsyncMock()
        servicer.driver.transaction = MagicMock(return_value=AsyncMock())
        servicer.proc = AsyncMock()
        servicer.proc.driver = servicer.driver
        servicer.storage = AsyncMock()
        servicer.cache = AsyncMock()
        yield servicer

    @pytest.fixture
    def field_value(self):
        field_value = FieldText(body="body", format=FieldText.PLAIN, md5="md5")
        yield field_value

    @pytest.fixture
    def field(self, field_value):
        val = Text("id", Mock(), value=field_value.SerializeToString())
        val.set_vectors = AsyncMock()
        yield val

    @pytest.fixture(scope="function")
    def knowledgebox_class(self):
        mock = AsyncMock()
        mock.new_unique_kbid.return_value = "kbid"
        mock.create.return_value = ("kbid", "slug")
        with patch("nucliadb.ingest.service.writer.KnowledgeBoxORM", new=mock):
            yield mock

    @pytest.fixture(autouse=True)
    def resource(self, field):
        mock = AsyncMock()
        mock.get_field.return_value = field
        with patch("nucliadb.ingest.service.writer.ResourceORM", return_value=mock):
            yield mock

    async def test_NewKnowledgeBox(self, writer: WriterServicer, hosted_nucliadb, knowledgebox_class):
        request = writer_pb2.KnowledgeBoxNew(
            slug="slug",
            forceuuid="kbid",
            similarity=VectorSimilarity.DOT,
            vector_dimension=200,
        )

        resp = await writer.NewKnowledgeBox(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK
        assert knowledgebox_class.create.call_count == 1
        assert knowledgebox_class.create.call_args.kwargs["slug"] == request.slug
        assert knowledgebox_class.create.call_args.kwargs["semantic_model"] == SemanticModelMetadata(
            similarity_function=request.similarity,
            vector_dimension=request.vector_dimension,
        )
        assert knowledgebox_class.create.call_args.kwargs["title"] == request.config.title
        assert knowledgebox_class.create.call_args.kwargs["description"] == request.config.description
        assert knowledgebox_class.create.call_args.kwargs["release_channel"] == request.release_channel

    async def test_NewKnowledgeBoxV2(self, writer: WriterServicer, hosted_nucliadb, knowledgebox_class):
        request = writer_pb2.NewKnowledgeBoxV2Request(
            kbid="kbid",
            slug="slug",
            title="Title",
            description="Description",
            vectorsets=[
                writer_pb2.NewKnowledgeBoxV2Request.VectorSet(
                    vectorset_id="vectorset_id",
                    similarity=VectorSimilarity.DOT,
                    vector_dimension=200,
                )
            ],
        )

        resp = await writer.NewKnowledgeBoxV2(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK
        assert knowledgebox_class.create.call_count == 1
        assert knowledgebox_class.create.call_args.kwargs["kbid"] == request.kbid
        assert knowledgebox_class.create.call_args.kwargs["slug"] == request.slug
        assert knowledgebox_class.create.call_args.kwargs["title"] == request.title
        assert knowledgebox_class.create.call_args.kwargs["description"] == request.description
        assert knowledgebox_class.create.call_args.kwargs["semantic_models"] == {
            vs.vectorset_id: SemanticModelMetadata(
                similarity_function=vs.similarity,
                vector_dimension=vs.vector_dimension,
                matryoshka_dimensions=vs.matryoshka_dimensions,
            )
            for vs in request.vectorsets
        }
        assert "release_channel" not in knowledgebox_class.create.call_args.kwargs

    async def test_NewKnowledgeBox_experimental_channel(
        self, writer: WriterServicer, hosted_nucliadb, knowledgebox_class
    ):
        request = writer_pb2.KnowledgeBoxNew(
            slug="slug", release_channel=ReleaseChannel.EXPERIMENTAL, forceuuid="kbid"
        )

        resp = await writer.NewKnowledgeBox(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK
        assert (
            knowledgebox_class.create.call_args.kwargs["release_channel"] == ReleaseChannel.EXPERIMENTAL
        )

    async def test_NewKnowledgeBox_with_matryoshka_dimensions(
        self, writer: WriterServicer, hosted_nucliadb, knowledgebox_class
    ):
        request = writer_pb2.KnowledgeBoxNew(
            slug="slug",
            forceuuid="kbid",
            similarity=VectorSimilarity.COSINE,
            vector_dimension=200,
            matryoshka_dimensions=[200, 400],
        )

        resp = await writer.NewKnowledgeBox(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK
        assert knowledgebox_class.create.call_args.kwargs["semantic_model"] == SemanticModelMetadata(
            similarity_function=request.similarity,
            vector_dimension=request.vector_dimension,
            matryoshka_dimensions=request.matryoshka_dimensions,
        )

    async def test_NewKnowledgeBoxV2_with_matryoshka_dimensions(
        self, writer: WriterServicer, hosted_nucliadb, knowledgebox_class
    ):
        request = writer_pb2.NewKnowledgeBoxV2Request(
            kbid="kbid",
            slug="slug",
            title="Title",
            description="Description",
            vectorsets=[
                writer_pb2.NewKnowledgeBoxV2Request.VectorSet(
                    vectorset_id="vectorset_id",
                    similarity=VectorSimilarity.DOT,
                    vector_dimension=200,
                    matryoshka_dimensions=[200, 400],
                )
            ],
        )

        resp = await writer.NewKnowledgeBoxV2(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK
        assert knowledgebox_class.create.call_count == 1
        assert knowledgebox_class.create.call_args.kwargs["kbid"] == request.kbid
        assert knowledgebox_class.create.call_args.kwargs["slug"] == request.slug
        assert knowledgebox_class.create.call_args.kwargs["title"] == request.title
        assert knowledgebox_class.create.call_args.kwargs["description"] == request.description
        assert knowledgebox_class.create.call_args.kwargs["semantic_models"] == {
            vs.vectorset_id: SemanticModelMetadata(
                similarity_function=vs.similarity,
                vector_dimension=vs.vector_dimension,
                matryoshka_dimensions=vs.matryoshka_dimensions,
            )
            for vs in request.vectorsets
        }

    async def test_NewKnowledgeBoxV2_with_multiple_vectorsets(
        self, writer: WriterServicer, hosted_nucliadb, knowledgebox_class
    ):
        request = writer_pb2.NewKnowledgeBoxV2Request(
            kbid="kbid",
            slug="slug",
            title="Title",
            description="Description",
            vectorsets=[
                writer_pb2.NewKnowledgeBoxV2Request.VectorSet(
                    vectorset_id="vs1",
                    similarity=VectorSimilarity.DOT,
                    vector_dimension=200,
                    matryoshka_dimensions=[200, 400],
                ),
                writer_pb2.NewKnowledgeBoxV2Request.VectorSet(
                    vectorset_id="vs2",
                    similarity=VectorSimilarity.COSINE,
                    vector_dimension=500,
                ),
            ],
        )

        resp = await writer.NewKnowledgeBoxV2(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK
        assert knowledgebox_class.create.call_count == 1
        assert knowledgebox_class.create.call_args.kwargs["kbid"] == request.kbid
        assert knowledgebox_class.create.call_args.kwargs["slug"] == request.slug
        assert knowledgebox_class.create.call_args.kwargs["title"] == request.title
        assert knowledgebox_class.create.call_args.kwargs["description"] == request.description
        assert knowledgebox_class.create.call_args.kwargs["semantic_models"] == {
            vs.vectorset_id: SemanticModelMetadata(
                similarity_function=vs.similarity,
                vector_dimension=vs.vector_dimension,
                matryoshka_dimensions=vs.matryoshka_dimensions,
            )
            for vs in request.vectorsets
        }

    async def test_NewKnowledgeBox_handle_conflict_error(
        self, writer: WriterServicer, knowledgebox_class
    ):
        request = writer_pb2.KnowledgeBoxNew(slug="slug")
        knowledgebox_class.create.side_effect = KnowledgeBoxConflict()

        resp = await writer.NewKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.CONFLICT

    async def test_NewKnowledgeBoxV2_handle_conflict_error(
        self, writer: WriterServicer, knowledgebox_class
    ):
        request = writer_pb2.NewKnowledgeBoxV2Request(kbid="kbid", slug="slug")
        knowledgebox_class.create.side_effect = KnowledgeBoxConflict()

        resp = await writer.NewKnowledgeBoxV2(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.CONFLICT

    async def test_NewKnowledgeBox_handle_error(self, writer: WriterServicer, knowledgebox_class):
        request = writer_pb2.KnowledgeBoxNew(slug="slug")
        knowledgebox_class.create.side_effect = Exception("error")

        resp = await writer.NewKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_NewKnowledgeBoxV2_handle_error(self, writer: WriterServicer, knowledgebox_class):
        request = writer_pb2.NewKnowledgeBoxV2Request(kbid="kbid", slug="slug")
        knowledgebox_class.create.side_effect = Exception("error")

        resp = await writer.NewKnowledgeBoxV2(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_NewKnowledgeBox_handle_external_index_error(
        self, writer: WriterServicer, knowledgebox_class
    ):
        request = writer_pb2.KnowledgeBoxNew(slug="slug")
        knowledgebox_class.create.side_effect = ExternalIndexCreationError("pinecone", "foo")

        resp = await writer.NewKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.EXTERNAL_INDEX_PROVIDER_ERROR
        assert resp.error_message == "foo"

    async def test_NewKnowledgeBoxV2_handle_external_index_error(
        self, writer: WriterServicer, knowledgebox_class
    ):
        request = writer_pb2.NewKnowledgeBoxV2Request(kbid="kbid", slug="slug")
        knowledgebox_class.create.side_effect = ExternalIndexCreationError("pinecone", "foo")

        resp = await writer.NewKnowledgeBoxV2(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.EXTERNAL_INDEX_PROVIDER_ERROR
        assert resp.error_message == "foo"

    async def test_UpdateKnowledgeBox(self, writer: WriterServicer, knowledgebox_class):
        request = writer_pb2.KnowledgeBoxUpdate(slug="slug", uuid="uuid")
        knowledgebox_class.update.return_value = "kbid"

        resp = await writer.UpdateKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK
        assert knowledgebox_class.update.call_count == 1
        assert knowledgebox_class.update.call_args.kwargs["uuid"] == request.uuid
        assert knowledgebox_class.update.call_args.kwargs["slug"] == request.slug
        assert knowledgebox_class.update.call_args.kwargs["config"] == request.config

    async def test_UpdateKnowledgeBox_not_found(self, writer: WriterServicer, knowledgebox_class):
        request = writer_pb2.KnowledgeBoxUpdate(slug="slug", uuid="uuid")
        knowledgebox_class.update.side_effect = KnowledgeBoxNotFound()

        resp = await writer.UpdateKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.NOTFOUND

    async def test_UpdateKnowledgeBox_error(self, writer: WriterServicer, knowledgebox_class):
        request = writer_pb2.KnowledgeBoxUpdate(slug="slug")
        knowledgebox_class.update.side_effect = Exception()

        resp = await writer.UpdateKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_DeleteKnowledgeBox(self, writer: WriterServicer, knowledgebox_class):
        request = writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")

        resp = await writer.DeleteKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK
        assert knowledgebox_class.delete.call_count == 1
        assert knowledgebox_class.delete.call_args.kwargs["kbid"] == request.uuid

    async def test_DeleteKnowledgeBox_handle_error(self, writer: WriterServicer, knowledgebox_class):
        request = writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        knowledgebox_class.delete.side_effect = Exception()

        resp = await writer.DeleteKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_NewKnowledgeBox_not_available_for_onprem(
        self, writer: WriterServicer, onprem_nucliadb
    ):
        request = writer_pb2.KnowledgeBoxNew(slug="slug", forceuuid="kbid")
        resp = await writer.NewKnowledgeBox(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_UpdateKnowledgeBox_not_available_for_onprem(
        self, writer: WriterServicer, onprem_nucliadb
    ):
        request = writer_pb2.KnowledgeBoxUpdate(slug="slug", uuid="uuid")
        resp = await writer.UpdateKnowledgeBox(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_DeleteKnowledgeBox_not_available_for_onprem(
        self, writer: WriterServicer, onprem_nucliadb
    ):
        request = writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        resp = await writer.DeleteKnowledgeBox(request)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_GetEntities(self, writer: WriterServicer):
        request = writer_pb2.GetEntitiesRequest(kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"))

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.GetEntities(request)

        entities_manager.get_entities.assert_called_once_with(resp)
        assert resp.status == writer_pb2.GetEntitiesResponse.Status.OK

    async def test_GetEntities_missing(self, writer: WriterServicer):
        request = writer_pb2.GetEntitiesRequest(kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"))
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.GetEntities(request)

        assert resp.status == writer_pb2.GetEntitiesResponse.Status.NOTFOUND

    async def test_GetEntities_handle_error(self, writer: WriterServicer):
        request = writer_pb2.GetEntitiesRequest(kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"))

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            entities_manager.get_entities.side_effect = Exception("error")
            resp = await writer.GetEntities(request)

        assert resp.status == writer_pb2.GetEntitiesResponse.Status.ERROR

    async def test_ListEntitiesGroups(self, writer: WriterServicer):
        request = writer_pb2.ListEntitiesGroupsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        )

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        entities_manager.list_entities_groups.return_value = {
            "name": writer_pb2.EntitiesGroupSummary(title="group")
        }
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.ListEntitiesGroups(request)

        entities_manager.list_entities_groups.assert_called_once()
        assert resp.status == writer_pb2.GetEntitiesResponse.Status.OK

    async def test_ListEntitiesGroups_missing(self, writer: WriterServicer):
        request = writer_pb2.ListEntitiesGroupsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        )
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.ListEntitiesGroups(request)

        assert resp.status == writer_pb2.GetEntitiesResponse.Status.NOTFOUND

    async def test_ListEntitiesGroups_handle_error(self, writer: WriterServicer):
        request = writer_pb2.ListEntitiesGroupsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        )

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        entities_manager.list_entities_groups.side_effect = Exception("error")
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.ListEntitiesGroups(request)

        assert resp.status == writer_pb2.GetEntitiesResponse.Status.ERROR

    async def test_GetEntitiesGroup(self, writer: WriterServicer):
        request = writer_pb2.GetEntitiesGroupRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        entities_manager.get_entities_group.return_value = writer_pb2.EntitiesGroup(title="group")
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.GetEntitiesGroup(request)

        entities_manager.get_entities_group.assert_called_once_with("group")
        assert resp.status == writer_pb2.GetEntitiesGroupResponse.Status.OK

    async def test_GetEntitiesGroup_missing(self, writer: WriterServicer):
        request = writer_pb2.GetEntitiesGroupRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.GetEntitiesGroup(request)

        assert resp.status == writer_pb2.GetEntitiesGroupResponse.Status.KB_NOT_FOUND

    async def test_GetEntitiesGroup_handle_error(self, writer: WriterServicer):
        request = writer_pb2.GetEntitiesGroupRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        entities_manager.get_entities_group.side_effect = Exception("error")
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.GetEntitiesGroup(request)

        assert resp.status == writer_pb2.GetEntitiesGroupResponse.Status.ERROR

    async def test_SetEntities(self, writer: WriterServicer):
        request = writer_pb2.SetEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.SetEntities(request)

        entities_manager.set_entities_group.assert_called_once_with(request.group, request.entities)
        assert resp.status == writer_pb2.OpStatusWriter.Status.OK

    async def test_SetEntities_missing(self, writer: WriterServicer):
        request = writer_pb2.SetEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.SetEntities(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.NOTFOUND

    async def test_SetEntities_handle_error(self, writer: WriterServicer):
        request = writer_pb2.SetEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        entities_manager.set_entities_group.side_effect = Exception("error")
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.SetEntities(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.ERROR

    async def test_DelEntities(self, writer: WriterServicer):
        request = writer_pb2.DelEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.DelEntities(request)

        entities_manager.delete_entities_group.assert_called_once_with(request.group)
        assert resp.status == writer_pb2.OpStatusWriter.Status.OK

    async def test_DelEntities_missing(self, writer: WriterServicer):
        request = writer_pb2.DelEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.DelEntities(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.NOTFOUND

    async def test_DelEntities_handle_error(self, writer: WriterServicer):
        request = writer_pb2.DelEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), group="group"
        )

        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        entities_manager = AsyncMock()
        entities_manager.delete_entities_group.side_effect = Exception("error")
        with patch(
            "nucliadb.ingest.service.writer.EntitiesManager",
            return_value=entities_manager,
        ):
            resp = await writer.DelEntities(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.ERROR

    async def test_Index(self, writer: WriterServicer):
        request = writer_pb2.IndexResource(kbid="kbid", rid="rid")

        txn = AsyncMock()
        with (
            patch("nucliadb.ingest.service.writer.get_partitioning") as get_partitioning,
            patch(
                "nucliadb.ingest.service.writer.get_transaction_utility",
                MagicMock(return_value=txn),
            ),
            patch(
                "nucliadb.ingest.service.writer.generate_broker_message",
                AsyncMock(return_value=writer_pb2.BrokerMessage()),
            ),
        ):
            resp = await writer.Index(request)

            get_partitioning().generate_partition.assert_called_once_with("kbid", "rid")
            txn.commit.assert_called_once()

            assert isinstance(resp, writer_pb2.IndexStatus)
