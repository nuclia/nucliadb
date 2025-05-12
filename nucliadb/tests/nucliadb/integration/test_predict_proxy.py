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


import pytest
from httpx import AsyncClient


@pytest.mark.parametrize(
    "method,endpoint,params,payload,headers",
    [
        ("GET", "tokens", {"text": "foo"}, None, {"foo": "bar"}),
        (
            "POST",
            "chat",
            None,
            {"question": "foo", "query_context": ["foobar"], "user_id": "foo"},
            {"foo": "bar"},
        ),
        (
            "POST",
            "rephrase",
            None,
            {
                "question": "baba",
                "chat_histoy": [{"author": "USER", "text": "foo"}],
                "user_id": "bar",
            },
            {"foo": "bar"},
        ),
    ],
)
@pytest.mark.deploy_modes("standalone")
async def test_predict_proxy(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    method: str,
    endpoint: str,
    params,
    payload,
    headers: dict[str, str],
):
    kbid = standalone_knowledgebox
    http_func = getattr(nucliadb_reader, method.lower())
    http_func_kwargs = {"params": params, "headers": headers}
    if method == "POST":
        http_func_kwargs["json"] = payload
    resp = await http_func(
        f"/kb/{kbid}/predict/{endpoint}",
        timeout=None,
        **http_func_kwargs,
    )
    assert resp.status_code == 200, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_predict_proxy_not_proxied_returns_422(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/predict/not-proxied-endpoint",
        json={"resources": {"foo": "bar"}},
    )
    assert resp.status_code == 422


@pytest.mark.deploy_modes("standalone")
async def test_predict_proxy_returns_404_on_non_existing_kb(
    nucliadb_reader: AsyncClient,
):
    resp = await nucliadb_reader.post(
        f"/kb/idonotexist-kb/predict/chat",
        json={"question": "foo", "query_context": ["foobar"], "user_id": "foo"},
    )
    assert resp.status_code == 404
