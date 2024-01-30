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
import platform
import sys
import tarfile
import tempfile
from collections.abc import AsyncGenerator
from typing import Optional

import pkg_resources
import psutil
from fastapi import FastAPI
from pydantic import BaseModel

from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.standalone.settings import Settings
from nucliadb_telemetry.settings import LogSettings

MB = 1024 * 1024
CHUNK_SIZE = 2 * MB
SYSTEM_INFO_TEMPLATE = """System info
===========

Python
------
    - Version: {python_version}

Operative system
----------------
    - Name: {os_name}
    - Release: {os_release}
    - Version: {os_version}
    - Machine: {os_machine}
    - File System Encoding: {os_file_system_encoding}

CPU information
---------------
    - Number of CPUs: {cpu_count}

Memory information
------------------
    - Total: {memory_total:.2f} MB
    - Available: {memory_available:.2f} MB
    - Used: {memory_used:.2f} MB
    - Used %: {memory_used_percent:.2f}%
"""


class NodeInfo(BaseModel):
    id: str
    address: str
    shard_count: int
    primary_id: Optional[str]


class ClusterInfo(BaseModel):
    nodes: list[NodeInfo]


async def stream_tar(app: FastAPI) -> AsyncGenerator[bytes, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        tar_file = os.path.join(temp_dir, "introspect.tar.gz")
        with tarfile.open(tar_file, mode="w:gz") as tar:
            await add_system_info(temp_dir, tar)
            await add_dependencies(temp_dir, tar)
            await add_cluster_info(temp_dir, tar)
            settings: Settings = app.settings.copy()  # type: ignore
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


async def add_system_info(temp_dir: str, tar: tarfile.TarFile):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _add_system_info_to_tar, temp_dir, tar)


def _add_system_info_to_tar(temp_dir: str, tar: tarfile.TarFile):
    system_info_file = os.path.join(temp_dir, "system_info.txt")
    with open(system_info_file, "w") as f:
        memory = psutil.virtual_memory()
        f.write(
            SYSTEM_INFO_TEMPLATE.format(
                python_version=sys.version,
                os_name=os.uname().sysname,
                os_release=platform.release(),
                os_version=platform.version(),
                os_machine=platform.machine(),
                os_file_system_encoding=os.sys.getfilesystemencoding(),  # type: ignore
                cpu_count=psutil.cpu_count(),
                memory_total=memory.total / MB,
                memory_available=memory.available / MB,
                memory_used=memory.used / MB,
                memory_used_percent=memory.percent,
            )
        )
    tar.add(system_info_file, arcname="system_info.txt")


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


async def add_cluster_info(temp_dir: str, tar: tarfile.TarFile):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _add_cluster_info_to_tar, temp_dir, tar)


def _add_cluster_info_to_tar(temp_dir: str, tar: tarfile.TarFile):
    cluster_info = ClusterInfo(
        nodes=[
            NodeInfo(
                id=node.id,
                address=node.address,
                shard_count=node.shard_count,
                primary_id=node.primary_id,
            )
            for node in cluster_manager.get_index_nodes()
        ]
    )
    cluster_info_file = os.path.join(temp_dir, "cluster_info.txt")
    with open(cluster_info_file, "w") as f:
        f.write(cluster_info.json(indent=4))
    tar.add(cluster_info_file, arcname="cluster_info.txt")


async def add_settings(temp_dir: str, tar: tarfile.TarFile, settings: Settings):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _add_settings_to_tar, temp_dir, tar, settings)


def _add_settings_to_tar(temp_dir: str, tar: tarfile.TarFile, settings: Settings):
    remove_sensitive_settings(settings)
    settings_file = os.path.join(temp_dir, "settings.json")
    with open(settings_file, "w") as f:
        f.write(settings.json(indent=4))
    tar.add(settings_file, arcname="settings.json")


def remove_sensitive_settings(settings: Settings):
    for sensitive_setting in [
        "nua_api_key",
        "jwk_key",
        "gcs_base64_creds",
        "s3_client_secret",
        "driver_pg_url",
    ]:
        if hasattr(settings, sensitive_setting):
            setattr(settings, sensitive_setting, "********")


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
