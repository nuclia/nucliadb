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


from nucliadb_models.export_import import Status


class ExportStreamExhausted(Exception):
    """
    Raised when there are no more bytes to read from the export stream.
    """

    pass


class MetadataNotFound(Exception):
    """
    Raised when the metadata for an export/import is not found.
    """

    pass


class TaskNotFinishedError(Exception):
    """
    Raised when trying to get the status of a task that is not finished.
    """

    pass


class TaskErrored(Exception):
    """
    Raised when a task has errored.
    """

    pass


def raise_for_task_status(status: Status):
    if status == Status.FINISHED:
        return
    raise {
        Status.ERRORED: TaskErrored,
        Status.SCHEDULED: TaskNotFinishedError,
        Status.RUNNING: TaskNotFinishedError,
    }[status]


class WrongExportStreamFormat(Exception):
    """
    Raised then data being imported does not follow the expected format
    """

    pass
