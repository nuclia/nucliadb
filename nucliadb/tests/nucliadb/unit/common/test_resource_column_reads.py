from unittest.mock import AsyncMock, Mock

import pytest

from nucliadb.common.datamanagers import resources
from nucliadb.common.marklogic.uris import MarkLogicURIs


@pytest.mark.parametrize("columns", [("slug",), ("slug", "extra"), ("slug", "extra", "origin")])
@pytest.mark.parametrize("missing", [False, True])
async def test_resource_column_read_threshold(monkeypatch, columns, missing):
    txn = Mock()
    monkeypatch.setattr(resources, "_driver_txn", lambda *args, **kwargs: (Mock(), txn))
    content = None if missing else {"slug": "resource", "extra": None, "origin": None}
    read = AsyncMock(return_value=content)
    read_properties = AsyncMock(return_value=content)
    monkeypatch.setattr(resources.documents, "read", read)
    monkeypatch.setattr(resources.documents, "read_properties", read_properties)

    result = await resources.get(txn, kbid="kb", rid="rid", columns=columns)

    if len(columns) <= 2:
        read_properties.assert_awaited_once_with(txn, MarkLogicURIs.resource("rid"), columns)
        read.assert_not_awaited()
    else:
        read.assert_awaited_once_with(txn, MarkLogicURIs.resource("rid"))
        read_properties.assert_not_awaited()
    if missing:
        assert result is None
    else:
        assert result is not None
        assert result.slug == "resource"
        if "extra" in columns:
            assert result.extra is None
