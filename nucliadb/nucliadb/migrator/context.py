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

from nucliadb.common.context import ApplicationContext

from .datamanager import MigrationsDataManager
from .settings import Settings


class ExecutionContext(ApplicationContext):
    data_manager: MigrationsDataManager

    def __init__(self, settings: Settings) -> None:
        super().__init__(service_name="migrator")
        self.settings = settings

    async def initialize(self) -> None:
        await super().initialize()
        self.data_manager = MigrationsDataManager(self.kv_driver)
