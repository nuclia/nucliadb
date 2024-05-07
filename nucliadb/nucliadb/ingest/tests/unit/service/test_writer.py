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
import json
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxNew, SemanticModelMetadata
from nucliadb_protos.resources_pb2 import FieldID, FieldText, FieldType
from nucliadb_protos.utils_pb2 import ReleaseChannel, VectorSimilarity

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.service.writer import WriterServicer, get_release_channel
from nucliadb.learning_proxy import LearningConfiguration
from nucliadb_protos import writer_pb2


class TestWriterServicer:
    @pytest.fixture
    def onprem_nucliadb(self):
        with patch(
            "nucliadb.ingest.service.writer.is_onprem_nucliadb", return_value=True
        ) as mocked:
            yield mocked

    @pytest.fixture
    def learning_config(self):
        lconfig = LearningConfiguration(
            semantic_model="english",
            semantic_threshold=1,
            semantic_vector_size=200,
            semantic_vector_similarity="dot",
        )
        with patch("nucliadb.ingest.service.writer.learning_proxy") as mocked:
            mocked.get_configuration = AsyncMock(return_value=lconfig)
            yield mocked

    @pytest.fixture
    def writer(self, learning_config, onprem_nucliadb):
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

    @pytest.fixture(autouse=True)
    def resource(self, field):
        mock = AsyncMock()
        mock.get_field.return_value = field
        with patch("nucliadb.ingest.service.writer.ResourceORM", return_value=mock):
            yield mock

    async def test_SetVectors(self, writer: WriterServicer, resource):
        request = writer_pb2.SetVectorsRequest(
            kbid="kbid",
            rid="rid",
            field=FieldID(field_type=FieldType.TEXT, field="field"),
        )

        resp = await writer.SetVectors(request)

        txn = writer.driver.transaction.return_value.__aenter__.return_value
        txn.commit.assert_called_once()

        assert resp.found

    async def test_SetVectors_not_found(self, writer: WriterServicer, resource):
        request = writer_pb2.SetVectorsRequest(
            kbid="kbid",
            rid="rid",
            field=FieldID(field_type=FieldType.TEXT, field="field"),
        )

        resource.get_field.return_value = Text("id", Mock())
        resp = await writer.SetVectors(request)

        assert not resp.found

    async def test_SetVectors_error(self, writer: WriterServicer, resource, field):
        request = writer_pb2.SetVectorsRequest(
            kbid="kbid",
            rid="rid",
            field=FieldID(field_type=FieldType.TEXT, field="field"),
        )
        field.set_vectors.side_effect = Exception("error")

        resp = await writer.SetVectors(request)

        assert resp.found

    async def test_NewKnowledgeBox(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxNew(slug="slug", forceuuid="kbid")

        resp = await writer.NewKnowledgeBox(request)

        expected_model_metadata = SemanticModelMetadata(
            similarity_function=VectorSimilarity.DOT,
            vector_dimension=200,
        )
        writer.proc.create_kb.assert_called_once_with(
            request.slug,
            request.config,
            expected_model_metadata,
            forceuuid="kbid",
            release_channel=0,
        )
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK

    async def test_NewKnowledgeBox_experimental_channel(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxNew(
            slug="slug", release_channel=ReleaseChannel.EXPERIMENTAL, forceuuid="kbid"
        )

        resp = await writer.NewKnowledgeBox(request)

        expected_model_metadata = SemanticModelMetadata(
            similarity_function=VectorSimilarity.DOT,
            vector_dimension=200,
        )
        writer.proc.create_kb.assert_called_once_with(
            request.slug,
            request.config,
            expected_model_metadata,
            forceuuid=request.forceuuid,
            release_channel=1,
        )
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK

    async def test_NewKnowledgeBox_hosted_nucliadb(
        self, writer: WriterServicer, onprem_nucliadb
    ):
        onprem_nucliadb.return_value = False

        request = writer_pb2.KnowledgeBoxNew(
            slug="slug",
            forceuuid="kbid",
            similarity=VectorSimilarity.COSINE,
            vector_dimension=200,
        )

        resp = await writer.NewKnowledgeBox(request)

        expected_model_metadata = SemanticModelMetadata(
            similarity_function=VectorSimilarity.COSINE,
            vector_dimension=200,
        )
        writer.proc.create_kb.assert_called_once_with(
            request.slug,
            request.config,
            expected_model_metadata,
            forceuuid="kbid",
            release_channel=0,
        )
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK

    async def test_NewKnowledgeBox_hosted_nucliadb_with_matryoshka_dimensions(
        self, writer: WriterServicer, onprem_nucliadb
    ):
        onprem_nucliadb.return_value = False

        request = writer_pb2.KnowledgeBoxNew(
            slug="slug",
            forceuuid="kbid",
            similarity=VectorSimilarity.COSINE,
            vector_dimension=200,
            matryoshka_dimensions=[200, 400],
        )

        resp = await writer.NewKnowledgeBox(request)

        expected_model_metadata = SemanticModelMetadata(
            similarity_function=VectorSimilarity.COSINE,
            vector_dimension=200,
            matryoshka_dimensions=[200, 400],
        )
        writer.proc.create_kb.assert_called_once_with(
            request.slug,
            request.config,
            expected_model_metadata,
            forceuuid="kbid",
            release_channel=0,
        )
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK

    async def test_NewKnowledgeBox_with_learning_config(
        self, writer: WriterServicer, learning_config
    ):
        learning_config.get_configuration.return_value = None
        learning_config.set_configuration = AsyncMock(
            return_value=LearningConfiguration(
                semantic_model="multilingual",
                semantic_threshold=-1,
                semantic_vector_size=10,
                semantic_vector_similarity="cosine",
            )
        )

        request = writer_pb2.KnowledgeBoxNew(
            slug="slug2",
            forceuuid="kbid",
            learning_config=json.dumps({"semantic_model": "multilingual"}),
        )

        resp = await writer.NewKnowledgeBox(request)

        expected_model_metadata = SemanticModelMetadata(
            similarity_function=VectorSimilarity.COSINE,
            vector_dimension=10,
        )
        writer.proc.create_kb.assert_called_once_with(
            request.slug,
            request.config,
            expected_model_metadata,
            forceuuid=request.forceuuid,
            release_channel=0,
        )
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK

    async def test_NewKnowledgeBox_with_learning_config_with_matryoshka_dimensions(
        self, writer: WriterServicer, learning_config
    ):
        learning_config.get_configuration.return_value = None
        learning_config.set_configuration = AsyncMock(
            return_value=LearningConfiguration(
                semantic_model="multilingual",
                semantic_threshold=-1,
                semantic_vector_size=10,
                semantic_vector_similarity="cosine",
                semantic_matryoshka_dims=[10, 20, 30],
            )
        )

        request = writer_pb2.KnowledgeBoxNew(
            slug="slug2",
            forceuuid="kbid",
            learning_config=json.dumps({"semantic_model": "multilingual"}),
        )

        resp = await writer.NewKnowledgeBox(request)

        expected_model_metadata = SemanticModelMetadata(
            similarity_function=VectorSimilarity.COSINE,
            vector_dimension=10,
            matryoshka_dimensions=[10, 20, 30],
        )
        writer.proc.create_kb.assert_called_once_with(
            request.slug,
            request.config,
            expected_model_metadata,
            forceuuid=request.forceuuid,
            release_channel=0,
        )
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK

    async def test_NewKnowledgeBox_handle_error(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxNew(slug="slug")
        writer.proc.create_kb.side_effect = Exception("error")

        resp = await writer.NewKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_UpdateKnowledgeBox(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxUpdate(slug="slug", uuid="uuid")
        writer.proc.update_kb.return_value = "kbid"

        resp = await writer.UpdateKnowledgeBox(request)

        writer.proc.update_kb.assert_called_once_with(
            request.uuid, request.slug, request.config
        )
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK

    async def test_UpdateKnowledgeBox_not_found(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxUpdate(slug="slug", uuid="uuid")
        writer.proc.update_kb.side_effect = KnowledgeBoxNotFound()

        resp = await writer.UpdateKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.NOTFOUND

    async def test_UpdateKnowledgeBox_error(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxUpdate(slug="slug")
        writer.proc.update_kb.side_effect = Exception()

        resp = await writer.UpdateKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_DeleteKnowledgeBox(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")

        resp = await writer.DeleteKnowledgeBox(request)

        writer.proc.delete_kb.assert_called_once_with(request.uuid)
        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.OK

    async def test_DeleteKnowledgeBox_handle_error(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        writer.proc.delete_kb.side_effect = Exception("error")

        resp = await writer.DeleteKnowledgeBox(request)

        assert resp.status == writer_pb2.KnowledgeBoxResponseStatus.ERROR

    async def test_GCKnowledgeBox(self, writer: WriterServicer):
        request = writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")

        resp = await writer.GCKnowledgeBox(request)

        assert isinstance(resp, writer_pb2.GCKnowledgeBoxResponse)

    async def test_SetLabels(self, writer: WriterServicer):
        request = writer_pb2.SetLabelsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), id="id"
        )

        resp = await writer.SetLabels(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.OK
        txn = writer.driver.transaction.return_value.__aenter__.return_value
        txn.commit.assert_called_once()

    async def test_SetLabels_missing(self, writer: WriterServicer):
        request = writer_pb2.SetLabelsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), id="id"
        )
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.SetLabels(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.NOTFOUND

    async def test_SetLabels_handle_error(self, writer: WriterServicer):
        request = writer_pb2.SetLabelsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), id="id"
        )
        writer.proc.get_kb_obj.return_value.set_labelset.side_effect = Exception(
            "error"
        )

        resp = await writer.SetLabels(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.ERROR

    async def test_DelLabels(self, writer: WriterServicer):
        request = writer_pb2.DelLabelsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), id="id"
        )

        resp = await writer.DelLabels(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.OK
        txn = writer.driver.transaction.return_value.__aenter__.return_value
        txn.commit.assert_called_once()

    async def test_DelLabels_missing(self, writer: WriterServicer):
        request = writer_pb2.DelLabelsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), id="id"
        )
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.DelLabels(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.NOTFOUND

    async def test_DelLabels_handle_error(self, writer: WriterServicer):
        request = writer_pb2.DelLabelsRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), id="id"
        )
        writer.proc.get_kb_obj.return_value.del_labelset.side_effect = Exception(
            "error"
        )

        resp = await writer.DelLabels(request)

        assert resp.status == writer_pb2.OpStatusWriter.Status.ERROR

    async def test_GetLabelSet(self, writer: WriterServicer):
        request = writer_pb2.GetLabelSetRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), labelset="labelset"
        )
        writer.proc.get_kb_obj.return_value = AsyncMock(kbid="kbid")

        resp = await writer.GetLabelSet(request)

        assert resp.status == writer_pb2.GetLabelSetResponse.Status.OK

    async def test_GetLabelSet_kb_missing(self, writer: WriterServicer):
        request = writer_pb2.GetLabelSetRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid"), labelset="labelset"
        )
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.GetLabelSet(request)

        assert resp.status == writer_pb2.GetLabelSetResponse.Status.NOTFOUND

    async def test_GetEntities(self, writer: WriterServicer):
        request = writer_pb2.GetEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        )

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
        request = writer_pb2.GetEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        )
        writer.proc.get_kb_obj.return_value = None

        resp = await writer.GetEntities(request)

        assert resp.status == writer_pb2.GetEntitiesResponse.Status.NOTFOUND

    async def test_GetEntities_handle_error(self, writer: WriterServicer):
        request = writer_pb2.GetEntitiesRequest(
            kb=writer_pb2.KnowledgeBoxID(slug="slug", uuid="uuid")
        )

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
        entities_manager.get_entities_group.return_value = writer_pb2.EntitiesGroup(
            title="group"
        )
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

        entities_manager.set_entities_group.assert_called_once_with(
            request.group, request.entities
        )
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
        with patch(
            "nucliadb.ingest.service.writer.get_partitioning"
        ) as get_partitioning, patch(
            "nucliadb.ingest.service.writer.get_transaction_utility",
            MagicMock(return_value=txn),
        ):
            resp = await writer.Index(request)

            get_partitioning().generate_partition.assert_called_once_with("kbid", "rid")
            txn.commit.assert_called_once()

            assert isinstance(resp, writer_pb2.IndexStatus)


@pytest.mark.parametrize(
    "req,has_feature,environment,expected_channel",
    [
        (
            KnowledgeBoxNew(slug="foo", release_channel=ReleaseChannel.EXPERIMENTAL),
            False,
            "prod",
            ReleaseChannel.EXPERIMENTAL,
        ),
        (
            KnowledgeBoxNew(slug="foo", release_channel=ReleaseChannel.STABLE),
            True,
            "prod",
            ReleaseChannel.STABLE,
        ),
        (
            KnowledgeBoxNew(slug="foo", release_channel=ReleaseChannel.STABLE),
            True,
            "stage",
            ReleaseChannel.EXPERIMENTAL,
        ),
        (
            KnowledgeBoxNew(slug="foo", release_channel=ReleaseChannel.STABLE),
            False,
            "stage",
            ReleaseChannel.STABLE,
        ),
    ],
)
def test_get_release_channel(req, has_feature, environment, expected_channel):
    module = "nucliadb.ingest.service.writer"
    with mock.patch(f"{module}.has_feature", return_value=has_feature):
        with mock.patch(
            f"{module}.running_settings", new=Mock(running_environment=environment)
        ):
            assert get_release_channel(req) == expected_channel
