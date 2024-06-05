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

from nucliadb.standalone.introspect import remove_sensitive_settings
from nucliadb.standalone.settings import Settings


def test_remove_sensitive_settings():
    sensitive = dict(
        nua_api_key="secret",
        jwk_key="secret",
        gcs_base64_creds="secret",
        s3_client_secret="secret",
        driver_pg_url="secret",
    )
    settings = Settings(**sensitive)
    remove_sensitive_settings(settings)
    for key in sensitive:
        assert getattr(settings, key) == "********"
