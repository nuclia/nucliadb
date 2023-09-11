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

import base64
from enum import Enum

from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    GetEntitiesResponse,
    GetLabelsResponse,
)

BINARY_SEP = "__X__"


class CODEX(str, Enum):
    RESOURCE = "RES"
    LABELS = "LAB"
    ENTITIES = "ENT"
    BINARY = "BIN"


def encode_bm(bm: BrokerMessage) -> str:
    return CODEX.RESOURCE + base64.b64encode(bm.SerializeToString()).decode()


def encode_entities(entities: GetEntitiesResponse) -> str:
    return CODEX.ENTITIES + base64.b64encode(entities.SerializeToString()).decode()


def encode_labels(labels: GetLabelsResponse) -> str:
    return CODEX.LABELS + base64.b64encode(labels.SerializeToString()).decode()


def encode_binary(binary: bytes, rid: str) -> str:
    return CODEX.BINARY + rid + BINARY_SEP + base64.b64encode(binary).decode()


def decode_bm(line: str) -> BrokerMessage:
    bm = BrokerMessage()
    payload = base64.b64decode(line[3:])
    bm.ParseFromString(payload)
    return bm


def decode_entities(line: str) -> GetEntitiesResponse:
    entities = GetEntitiesResponse()
    payload = base64.b64decode(line[3:])
    entities.ParseFromString(payload)
    return entities


def decode_labels(line: str) -> GetLabelsResponse:
    labels = GetLabelsResponse()
    payload = base64.b64decode(line[3:])
    labels.ParseFromString(payload)
    return labels


def decode_binary(line: str) -> tuple[str, bytes]:
    rid, binary = line[3:].split(BINARY_SEP)
    return rid, base64.b64decode(binary)


def get_type(line: str) -> CODEX:
    return CODEX(line[:3])
