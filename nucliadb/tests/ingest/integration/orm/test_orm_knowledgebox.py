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
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict, KnowledgeBoxCreationError
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox, chunker
from nucliadb_protos import utils_pb2
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility
from tests.ingest.fixtures import broker_resource


@pytest.fixture(scope="function")
async def shard_manager(
    storage: Storage,
    maindb_driver: Driver,
):
    manager = AsyncMock()
    original = get_utility(Utility.SHARD_MANAGER)
    set_utility(Utility.SHARD_MANAGER, manager)

    yield manager

    if original is None:
        clean_utility(Utility.SHARD_MANAGER)
    else:
        set_utility(Utility.SHARD_MANAGER, original)


@pytest.mark.asyncio
async def test_create_knowledgebox(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: cluster_manager.KBShardManager,
):
    kbid = KnowledgeBox.new_unique_kbid()
    slug = f"slug-{kbid}"
    title = "KB title"
    description = "KB description"

    # DEPRECATED creation using semantic_model instead of semantic_models
    result = await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=slug,
        title=title,
        description=description,
        semantic_model=SemanticModelMetadata(),
    )
    assert result == (kbid, slug)
    async with maindb_driver.transaction(read_only=True) as txn:
        exists = await datamanagers.kb.exists_kb(txn, kbid=kbid)
        assert exists

        config = await datamanagers.kb.get_config(txn, kbid=kbid)
        assert config is not None
        assert config.slug == slug
        assert config.title == title
        assert config.description == description


@pytest.mark.asyncio
async def test_create_knowledgebox_with_multiple_vectorsets(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: cluster_manager.KBShardManager,
):
    kbid = KnowledgeBox.new_unique_kbid()
    slug = f"slug-{kbid}"
    result = await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=slug,
        semantic_models={
            "vs1": SemanticModelMetadata(
                vector_dimension=200,
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                default_min_score=0.78,
            ),
            "vs2": SemanticModelMetadata(
                vector_dimension=512,
                similarity_function=utils_pb2.VectorSimilarity.DOT,
                default_min_score=0.78,
                matryoshka_dimensions=[256, 512, 2048],
            ),
        },
    )
    assert result == (kbid, slug)
    async with maindb_driver.transaction(read_only=True) as txn:
        exists = await datamanagers.kb.exists_kb(txn, kbid=kbid)
        assert exists

        assert len([vs async for vs in datamanagers.vectorsets.iter(txn, kbid=kbid)]) == 2

        vs1 = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id="vs1")
        assert vs1 is not None
        assert vs1.vectorset_id == "vs1"
        assert vs1.vectorset_index_config.vector_dimension == 200
        assert vs1.vectorset_index_config.similarity == utils_pb2.VectorSimilarity.COSINE
        assert vs1.vectorset_index_config.normalize_vectors is False
        assert len(vs1.matryoshka_dimensions) == 0

        vs2 = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id="vs2")
        assert vs2 is not None
        assert vs2.vectorset_id == "vs2"
        assert vs2.vectorset_index_config.vector_dimension == 512
        assert vs2.vectorset_index_config.similarity == utils_pb2.VectorSimilarity.DOT
        assert vs2.vectorset_index_config.normalize_vectors is True
        assert set(vs2.matryoshka_dimensions) == {256, 512, 2048}


@pytest.mark.asyncio
async def test_create_knowledgebox_without_vectorsets_is_not_allowed(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: cluster_manager.KBShardManager,
):
    with pytest.raises(KnowledgeBoxCreationError):
        await KnowledgeBox.create(maindb_driver, kbid="kbid", slug="slug", semantic_models={})


@pytest.mark.parametrize(
    "release_channel",
    [
        utils_pb2.ReleaseChannel.STABLE,
        utils_pb2.ReleaseChannel.EXPERIMENTAL,
    ],
)
@pytest.mark.asyncio
async def test_create_knowledgebox_with_release_channel(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: cluster_manager.KBShardManager,
    release_channel: utils_pb2.ReleaseChannel.ValueType,
):
    with patch("nucliadb.ingest.orm.knowledgebox.release_channel_for_kb") as mock:
        mock.return_value = release_channel

        kbid, _ = await KnowledgeBox.create(
            maindb_driver,
            kbid=KnowledgeBox.new_unique_kbid(),
            slug="mykbslug",
            semantic_model=SemanticModelMetadata(),
        )

    async with maindb_driver.transaction(read_only=True) as txn:
        shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        assert shards is not None
        assert shards.release_channel == release_channel


@pytest.mark.asyncio
async def test_create_knowledgebox_with_same_kbid(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: cluster_manager.KBShardManager,
):
    kbid = KnowledgeBox.new_unique_kbid()

    result_kbid, _ = await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=str(uuid.uuid4()),
        semantic_models={"vs": SemanticModelMetadata()},
    )
    assert result_kbid == kbid

    with pytest.raises(KnowledgeBoxConflict):
        await KnowledgeBox.create(
            maindb_driver,
            kbid=kbid,
            slug=str(uuid.uuid4()),
            semantic_models={"vs": SemanticModelMetadata()},
        )


@pytest.mark.asyncio
async def test_create_knowledgebox_with_same_slug(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: cluster_manager.KBShardManager,
):
    slug = "my-kb-slug"

    _, result_slug = await KnowledgeBox.create(
        maindb_driver,
        kbid=KnowledgeBox.new_unique_kbid(),
        slug=slug,
        semantic_models={"vs": SemanticModelMetadata()},
    )
    assert result_slug == slug

    with pytest.raises(KnowledgeBoxConflict):
        await KnowledgeBox.create(
            maindb_driver,
            kbid=KnowledgeBox.new_unique_kbid(),
            slug=slug,
            semantic_models={"vs": SemanticModelMetadata()},
        )


@pytest.mark.asyncio
async def test_delete_knowledgebox(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: cluster_manager.KBShardManager,
):
    kbid, _ = await KnowledgeBox.create(
        maindb_driver,
        kbid=KnowledgeBox.new_unique_kbid(),
        slug="my-kb",
        semantic_models={"vs": SemanticModelMetadata()},
    )
    exists = await datamanagers.atomic.kb.exists_kb(kbid=kbid)
    assert exists is True

    deleted_kbid = await KnowledgeBox.delete(maindb_driver, kbid)
    assert deleted_kbid == kbid

    exists = await datamanagers.atomic.kb.exists_kb(kbid=kbid)
    assert exists is False


@pytest.mark.asyncio
async def test_delete_knowledgebox_handles_unexisting_kb(storage: Storage, maindb_driver: Driver):
    kbid = await KnowledgeBox.delete(maindb_driver, kbid="idonotexist")
    assert kbid is None


@pytest.mark.asyncio
async def test_knowledgebox_purge_handles_unexisting_shard_payload(
    storage: Storage, maindb_driver: Driver
):
    await KnowledgeBox.purge(maindb_driver, "idonotexist")


@pytest.mark.asyncio
async def test_knowledgebox_delete_all_kb_keys(
    storage,
    cache,
    fake_node,
    maindb_driver,
    knowledgebox_ingest: str,
):
    async with maindb_driver.transaction() as txn:
        kbid = knowledgebox_ingest
        kb_obj = KnowledgeBox(txn, storage, kbid=kbid)

        # Create some resources in the KB
        n_resources = 100
        rids_and_slugs = set()
        for _ in range(n_resources):
            bm = broker_resource(kbid)
            rid = bm.uuid
            slug = f"slug-{rid}"
            bm.basic.slug = slug
            resource = await kb_obj.add_resource(uuid=rid, slug=slug, basic=bm.basic)
            assert resource is not None
            await resource.set_slug()
            rids_and_slugs.add((rid, slug))
        await txn.commit()

    # Check that all of them are there
    async with maindb_driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=kbid)
        for rid, slug in rids_and_slugs:
            assert await kb_obj.get_resource_uuid_by_slug(slug) == rid
        await txn.abort()

    # Now delete all kb keys
    await KnowledgeBox._delete_all_kb_keys(maindb_driver, kbid, chunk_size=10)

    # Check that all of them were deleted
    async with maindb_driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=kbid)
        for rid, slug in rids_and_slugs:
            assert await kb_obj.get_resource_uuid_by_slug(slug) is None
        await txn.abort()


def test_chunker():
    total_items = 100
    chunk_size = 10
    iterations = 0
    for chunk in chunker(list(range(total_items)), chunk_size):
        assert len(chunk) == chunk_size
        assert chunk == list(range(iterations * chunk_size, (iterations * chunk_size) + chunk_size))
        iterations += 1

    assert iterations == total_items / chunk_size

    iterations = 0
    for chunk in chunker([], 2):
        iterations += 1
    assert iterations == 0
