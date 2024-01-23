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

from molotov import global_setup, global_teardown, scenario

from nucliadb_performance.settings import get_predict_api_url
from nucliadb_performance.utils.errors import print_errors
from nucliadb_performance.utils.misc import (
    get_kb_to_test,
    get_request,
    make_kbid_request,
)
from nucliadb_performance.utils.vectors import predict_sentence_to_vector


@global_setup()
def init_test(args):
    get_kb_to_test()


@scenario(weight=1)
async def test_search(session):
    kbid, slug = get_kb_to_test()
    request = get_request(slug)
    predict_url = get_predict_api_url()
    vector = await predict_sentence_to_vector(
        predict_url, kbid, request.payload["query"]
    )
    request.payload["vector"] = vector
    await make_kbid_request(
        session,
        kbid,
        request.method.upper(),
        request.url.format(kbid=kbid),
        json=request.payload,
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    print_errors()
