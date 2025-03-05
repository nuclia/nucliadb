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


class MaindbKeys:
    METADATA = "kbs/{kbid}/backups/{backup_id}"
    LAST_RESTORED = "kbs/{kbid}/backup/{backup_id}/last_restored"


class StorageKeys:
    """
    Defines the key templates used to store backup files in the backups bucket of the storage.
    """

    BACKUP_PREFIX = "backups/{backup_id}/"
    RESOURCES_PREFIX = "backups/{backup_id}/resources/"
    RESOURCE = "backups/{backup_id}/resources/{resource_id}.tar"
    ENTITIES = "backups/{backup_id}/entities.pb"
    LABELS = "backups/{backup_id}/labels.pb"


class BackupFinishedStream:
    name = "backups"
    subject = "backups.creation_finished"
