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

import os
import tarfile
import tempfile

import pytest

from nucliadb.standalone.settings import Settings


@pytest.mark.asyncio
async def test_introspect_endpoint(nucliadb_manager) -> None:
    # Generate some traffic to have some logs
    await nucliadb_manager.post("/not/found")
    await nucliadb_manager.delete("/kb/foobar")

    resp = await nucliadb_manager.get("/introspect", timeout=600)
    assert resp.status_code == 200

    with tempfile.TemporaryDirectory() as root_dir:
        # Save the tar to a file
        introspect_tar_file = os.path.join(root_dir, "introspect.tar.gz")
        with open(introspect_tar_file, "wb") as f:
            f.write(resp.content)

        # Extract the tar
        extracted_tar = os.path.join(root_dir, "introspect")
        with tarfile.open(introspect_tar_file, "r:gz") as tar:
            tar.extractall(extracted_tar)

        # Check system info
        assert os.path.exists(os.path.join(extracted_tar, "system_info.txt"))

        # Check dependencies
        assert os.path.exists(os.path.join(extracted_tar, "dependencies.txt"))
        with open(os.path.join(extracted_tar, "dependencies.txt")) as f:
            dependencies = f.read()
            assert "nucliadb" in dependencies
            assert "nucliadb-models" in dependencies

        # Check settings
        assert os.path.exists(os.path.join(extracted_tar, "settings.json"))
        introspect_settings = Settings.parse_file(
            os.path.join(extracted_tar, "settings.json")
        )
        # Check that sensitive data is not included
        assert introspect_settings.nua_api_key is None
        assert introspect_settings.jwk_key is None
        assert introspect_settings.gcs_base64_creds is None
        assert introspect_settings.s3_client_secret is None

        # Check logs
        assert os.path.exists(os.path.join(extracted_tar, "logs/info.log"))
        assert os.path.exists(os.path.join(extracted_tar, "logs/error.log"))
        assert os.path.exists(os.path.join(extracted_tar, "logs/access.log"))
        assert os.path.getsize(os.path.join(extracted_tar, "logs/access.log")) > 0
