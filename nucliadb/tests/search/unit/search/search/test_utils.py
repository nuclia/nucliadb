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
import unittest

from nucliadb.search.search.utils import (
    maybe_log_request_payload,
)
from nucliadb_models.search import SearchRequest


def test_maybe_log_request_payload():
    with unittest.mock.patch("nucliadb.search.search.utils.logger") as mock_logger:
        with unittest.mock.patch("nucliadb.search.search.utils.has_feature") as mock_feature:
            mock_feature.return_value = True
            maybe_log_request_payload(
                "kbid",
                "/endpoint",
                SearchRequest(query="query", vector=[1.0, 2.0]),
            )
            assert mock_logger.info.call_count == 1

            mock_feature.return_value = False

            maybe_log_request_payload(
                "kbid",
                "/endpoint",
                SearchRequest(query="query", vector=[1.0, 2.0]),
            )
            assert mock_logger.info.call_count == 1
