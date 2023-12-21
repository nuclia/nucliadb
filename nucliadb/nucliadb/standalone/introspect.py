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

import asyncio
import os
import tarfile
import tempfile
from collections.abc import AsyncGenerator

import pkg_resources
from fastapi import FastAPI

from nucliadb.standalone.settings import Settings
from nucliadb_telemetry.settings import LogSettings

MB = 1024 * 1024
CHUNK_SIZE = 2 * MB


async def stream_tar(app: FastAPI) -> AsyncGenerator[bytes, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        tar_file = os.path.join(temp_dir, "introspect.tar.gz")
        with tarfile.open(tar_file, mode="w:gz") as tar:
            await add_dependencies(temp_dir, tar)
            if hasattr(app, "settings"):
                settings: Settings = app.settings.copy()
                await add_settings(temp_dir, tar, settings)
                if settings.log_output_type == "file":
                    await add_logs(tar)

        async for chunk in stream_out_tar(tar_file):
            yield chunk


async def stream_out_tar(tar_file: str) -> AsyncGenerator[bytes, None]:
    loop = asyncio.get_event_loop()
    with open(tar_file, "rb") as f:
        chunk = await loop.run_in_executor(None, f.read, CHUNK_SIZE)
        while chunk:
            yield chunk
            chunk = await loop.run_in_executor(None, f.read, CHUNK_SIZE)


async def add_dependencies(temp_dir: str, tar: tarfile.TarFile):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _add_dependencies_to_tar, temp_dir, tar)


def _add_dependencies_to_tar(temp_dir: str, tar: tarfile.TarFile):
    dependendies_file = os.path.join(temp_dir, "dependencies.txt")
    with open(dependendies_file, "w") as f:
        installed_packages = [pkg for pkg in pkg_resources.working_set]
        lines = []
        for pkg in sorted(installed_packages, key=lambda p: p.key):
            lines.append(f"{pkg.key}=={pkg.version}\n")
        f.writelines(lines)
    tar.add(dependendies_file, arcname="dependencies.txt")


async def add_settings(temp_dir: str, tar: tarfile.TarFile, settings: Settings):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _add_settings_to_tar, temp_dir, tar, settings)


def _add_settings_to_tar(temp_dir: str, tar: tarfile.TarFile, settings: Settings):
    # Remove sensitive data from settings
    settings.nua_api_key = None
    settings.jwk_key = None
    settings.gcs_base64_creds = None
    settings.s3_client_secret = None
    settings_file = os.path.join(temp_dir, "settings.json")
    with open(settings_file, "w") as f:
        f.write(settings.json(indent=4))
    tar.add(settings_file, arcname="settings.json")


async def add_logs(tar):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _add_logs_to_tar, tar)


def _add_logs_to_tar(tar: tarfile.TarFile):
    log_settings = LogSettings()
    access_log = os.path.realpath(log_settings.access_log)
    tar.add(access_log, arcname="logs/access.log")
    error_log = os.path.realpath(log_settings.error_log)
    tar.add(error_log, arcname="logs/error.log")
    info_log = os.path.realpath(log_settings.info_log)
    tar.add(info_log, arcname="logs/info.log")
