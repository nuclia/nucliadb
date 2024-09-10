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
import contextlib

from fastapi import HTTPException

from nucliadb.common import datamanagers, locking


@contextlib.asynccontextmanager
async def noop_context_manager():
    """
    This is used for requests where slug is not set by the user and we don't need to
    care about uniqueness
    """
    yield


@contextlib.asynccontextmanager
async def ensure_slug_uniqueness(kbid: str, slug: str):
    """
    Make sure slug is unique by:
    - First check if the slug is already taken by another existing resource
    - Otherwise, use the transaction lock to prevent from multiple concurrent
      create resource requests having the same slug.
    """
    try:
        async with locking.distributed_lock(
            key=locking.RESOURCE_CREATION_SLUG_LOCK.format(kbid=kbid, resource_slug=slug),
            # We don't want to wait here. If the lock exists, just raise exception
            lock_timeout=0.0,
            # Matches aprox the max amount of time that the ingest can take
            # to ingest a broker message from the writer.
            expire_timeout=60.0,
            # We don't want to refresh it here
            refresh_timeout=120.0,
        ):
            if await datamanagers.atomic.resources.slug_exists(kbid=kbid, slug=slug):
                raise HTTPException(status_code=409, detail=f"Resource slug {slug} already exists")
            yield
    except locking.ResourceLocked:
        raise HTTPException(
            status_code=409, detail=f"Another resource with the same {slug} is already being ingested"
        )
