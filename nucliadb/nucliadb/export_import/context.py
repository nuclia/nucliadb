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

from fastapi import FastAPI

from nucliadb.common.context import ApplicationContext
from nucliadb_utils.utilities import (
    start_partitioning_utility,
    start_transaction_utility,
    stop_partitioning_utility,
    stop_transaction_utility,
)

from .datamanager import ExporterDataManager, ImporterDataManager


class ExporterContext(ApplicationContext):
    data_manager: ExporterDataManager

    async def initialize(self) -> None:
        await super().initialize()
        self.data_manager = ExporterDataManager(self.kv_driver, self.blob_storage)

    async def finalize(self) -> None:
        await super().finalize()


class ImporterContext(ApplicationContext):
    data_manager: ImporterDataManager

    async def initialize(self) -> None:
        await super().initialize()
        self.data_manager = ImporterDataManager(
            self.kv_driver,
            self.blob_storage,
            start_partitioning_utility(),
            await start_transaction_utility(service_name="importer"),
        )

    async def finalize(self) -> None:
        await super().finalize()
        await stop_transaction_utility()
        stop_partitioning_utility()


def set_exporter_context_in_app(app: FastAPI, context: ExporterContext):
    app.state.exporter_context = context


def get_exporter_context_from_app(app: FastAPI) -> ExporterContext:
    return app.state.exporter_context


def set_importer_context_in_app(app: FastAPI, context: ImporterContext):
    app.state.importer_context = context


def get_importer_context_from_app(app: FastAPI) -> ImporterContext:
    return app.state.importer_context
