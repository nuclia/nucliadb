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

from unittest.mock import Mock, patch

import pytest

from nucliadb.ingest.orm.knowledgebox import release_channel_for_kb
from nucliadb_protos.utils_pb2 import ReleaseChannel


@pytest.mark.parametrize(
    "req,has_feature,environment,expected_channel",
    [
        (
            "foo",
            ReleaseChannel.EXPERIMENTAL,
            False,
            "prod",
            ReleaseChannel.EXPERIMENTAL,
        ),
        (
            "foo",
            ReleaseChannel.STABLE,
            True,
            "prod",
            ReleaseChannel.STABLE,
        ),
        (
            "foo",
            ReleaseChannel.STABLE,
            True,
            "stage",
            ReleaseChannel.EXPERIMENTAL,
        ),
        (
            "foo",
            ReleaseChannel.STABLE,
            False,
            "stage",
            ReleaseChannel.STABLE,
        ),
    ],
)
def test_release_channel_default_and_overwrite(
    slug: str, release_channel, has_feature, environment, expected_channel
):
    module = "nucliadb.ingest.orm.knowledgebox"
    with (
        patch(f"{module}.has_feature", return_value=has_feature),
        patch(f"{module}.running_settings", new=Mock(running_environment=environment)),
    ):
        assert release_channel_for_kb(slug, release_channel) == expected_channel
