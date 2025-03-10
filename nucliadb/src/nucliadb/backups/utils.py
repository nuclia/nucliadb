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

from nucliadb.backups.const import StorageKeys
from nucliadb.backups.settings import settings
from nucliadb_utils.storages.storage import Storage


async def exists_backup(storage: Storage, backup_id: str) -> bool:
    # As the labels file is always created, we use it to check if the backup exists
    return await storage.exists_object(
        settings.backups_bucket, StorageKeys.LABELS.format(backup_id=backup_id)
    )
