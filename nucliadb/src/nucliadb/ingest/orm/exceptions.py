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


class NotFound(Exception):
    pass


class KnowledgeBoxCreationError(Exception):
    pass


class KnowledgeBoxConflict(Exception):
    pass


class DeadletteredError(Exception):
    pass


class ReallyStopPulling(Exception):
    pass


class SequenceOrderViolation(Exception):
    def __init__(self, last_seqid: int):
        self.last_seqid = last_seqid


class ResourceNotIndexable(Exception):
    """
    Unable to index resource
    """

    def __init__(self, field_id: str, message: str):
        self.field_id = field_id
        self.message = message


class EntityManagementException(Exception):
    pass


class VectorSetConflict(Exception):
    pass


class InvalidBrokerMessage(ValueError):
    pass
