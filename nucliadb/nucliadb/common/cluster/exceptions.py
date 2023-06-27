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


class AlreadyExists(Exception):
    pass


class NotFound(Exception):
    pass


class NodeClusterSmall(Exception):
    pass


class ShardNotFound(NotFound):
    pass


class ShardsNotFound(NotFound):
    pass


class NodesUnsync(Exception):
    pass


class NodeError(Exception):
    pass


class ExhaustedNodesError(Exception):
    pass


class ReallyStopPulling(Exception):
    pass


class SequenceOrderViolation(Exception):
    def __init__(self, last_seqid: int):
        self.last_seqid = last_seqid


class EntitiesGroupNotFound(NotFound):
    pass
