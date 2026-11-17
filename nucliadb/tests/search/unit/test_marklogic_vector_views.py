from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.marklogic import vector_views
from nucliadb_protos.knowledgebox_pb2 import VectorSetConfig


def test_projected_keys_preserve_split_and_paragraph_offsets():
    columns = vector_views.template("model", 3)["template"]["rows"][0]["columns"]
    by_name = {column["name"]: column for column in columns}
    assert by_name["field_key"]["val"] == (
        "concat('/resources/', resource_id, '/fields/', field_type, '/', field_id, '/')"
    )
    paragraph = by_name["paragraph_key"]
    assert paragraph["nullable"] is True
    assert "count(tokenize(string(chunk_id), '/')) = 6" in paragraph["val"]
    assert "tokenize(string(chunk_id), '/')[4]" in paragraph["val"]
    assert "data/position/start, '-', data/position/end" in paragraph["val"]
    assert paragraph["val"].endswith("else ()")
    assert by_name["vector"]["annIndexed"] is True


@pytest.mark.parametrize("state", ["missing", "old", "current"])
async def test_ensure_view_reconciles_old_templates(monkeypatch, state):
    monkeypatch.setattr(vector_views, "_ensured", set())
    desired = vector_views.template("model", 3)
    existing = [] if state == "missing" else [{"template": {}}] if state == "old" else [desired]
    driver = Mock()
    driver.client.eval = AsyncMock(side_effect=[existing, []])

    await vector_views.ensure_view(driver, "kb-test", "model", 3)
    await vector_views.ensure_view(driver, "kb-test", "model", 3)

    assert driver.client.eval.await_count == (1 if state == "current" else 2)
    if state != "current":
        installation = driver.client.eval.await_args_list[1].kwargs
        assert installation["params"] == {"database": "kb-test"}
        assert "tde.templateInsert" in installation["javascript"]
        assert "paragraph_key" in installation["javascript"]


@pytest.mark.parametrize("current", [False, True])
async def test_forced_reconciliation_checks_cached_template(monkeypatch, current):
    monkeypatch.setattr(vector_views, "_ensured", {("model", 3)})
    existing = vector_views.template("model", 3) if current else {"template": {}}
    driver = Mock()
    driver.client.eval = AsyncMock(side_effect=[[existing], []])

    await vector_views.ensure_view(driver, "kb-test", "model", 3, force=True)

    assert driver.client.eval.await_count == (1 if current else 2)


@pytest.mark.parametrize("dimensions", [[], [3, 512]])
async def test_migration_reconciles_each_kb_vectorset(monkeypatch, dimensions):
    migration = import_module("migrations.0054_vector_prefilter_columns")
    driver = Mock(spec=MarkLogicDriver)
    transaction = Mock()
    driver.ro_transaction.return_value.__aenter__ = AsyncMock(return_value=transaction)
    driver.ro_transaction.return_value.__aexit__ = AsyncMock(return_value=False)
    driver.kb_database.return_value = "kb-database"

    async def iter_vectorsets(txn, *, kbid):
        assert txn is transaction
        assert kbid == "kb"
        for dimension in dimensions:
            config = VectorSetConfig(vectorset_id=f"model-{dimension}")
            config.vectorset_index_config.vector_dimension = dimension
            yield config.vectorset_id, config

    ensure = AsyncMock()
    monkeypatch.setattr(migration.datamanagers.vectorsets, "iter", iter_vectorsets)
    monkeypatch.setattr(vector_views, "ensure_view", ensure)

    await migration.migrate_kb(SimpleNamespace(kv_driver=driver), "kb")

    driver.ro_transaction.assert_called_once_with(kbid="kb")
    assert ensure.await_count == len(dimensions)
    for dimension in dimensions:
        ensure.assert_any_await(driver, "kb-database", f"model-{dimension}", dimension, force=True)


async def test_migration_skips_other_drivers(monkeypatch):
    migration = import_module("migrations.0054_vector_prefilter_columns")
    ensure = AsyncMock()
    monkeypatch.setattr(vector_views, "ensure_view", ensure)

    await migration.migrate_kb(SimpleNamespace(kv_driver=Mock()), "kb")

    ensure.assert_not_called()
