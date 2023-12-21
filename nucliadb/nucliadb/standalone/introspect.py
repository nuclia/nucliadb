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
from collections.abc import AsyncGenerator

import pkg_resources
from fastapi import FastAPI

from nucliadb.standalone.settings import Settings
from nucliadb_telemetry.settings import LogSettings

MB = 1024 * 1024
CHUNK_SIZE = 2 * MB


async def stream_tar(app: FastAPI) -> AsyncGenerator[bytes, None]:
    import tarfile
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create tar file
        tar_file = os.path.join(tmpdirname, "introspect.tar.gz")
        with tarfile.open(tar_file, mode="w:gz") as tar:
            # Add pip dependencies
            dependendies_file = os.path.join(tmpdirname, "dependencies.txt")
            with open(dependendies_file, "w") as f:
                installed_packages = [pkg for pkg in pkg_resources.working_set]
                lines = []
                for pkg in sorted(installed_packages, key=lambda p: p.key):
                    lines.append(f"{pkg.key}=={pkg.version}\n")
                f.writelines(lines)
            tar.add(dependendies_file, arcname="dependencies.txt")

            # Add standalone settings
            if not hasattr(app, "settings"):
                return

            settings: Settings = app.settings.copy()
            # Remove sensitive data
            settings.nua_api_key = None
            settings.jwk_key = None
            settings.gcs_base64_creds = None
            settings.s3_client_secret = None
            settings_file = os.path.join(tmpdirname, "settings.json")
            with open(settings_file, "w") as f:
                f.write(settings.json(indent=4))
            tar.add(settings_file, arcname="settings.json")

            # Add log files
            if settings.log_output_type == "file":
                log_settings = LogSettings()
                access_log = os.path.realpath(log_settings.access_log)
                tar.add(access_log, arcname="logs/access.log")
                error_log = os.path.realpath(log_settings.error_log)
                tar.add(error_log, arcname="logs/error.log")
                info_log = os.path.realpath(log_settings.info_log)
                tar.add(info_log, arcname="logs/info.log")

        # Stream out tar file
        with open(tar_file, "rb") as f:
            chunk = f.read(CHUNK_SIZE)
            while chunk:
                yield chunk
                chunk = f.read(CHUNK_SIZE)
