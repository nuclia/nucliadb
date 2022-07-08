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
from typing import Dict, List, Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    grpc_port: int = 8030
    train_grpc_address: Optional[str] = None

    root_certificates_file: Optional[str] = None
    private_key_file: Optional[str] = None
    certificate_chain_file: Optional[str] = None
    training_key: Optional[str] = None

    driver: str = "redis"  # redis | tikv
    driver_redis_url: Optional[str] = None
    driver_tikv_url: Optional[List[str]] = []
    driver_local_url: Optional[str] = None

    logging_config: Optional[str] = None

    node_writer_port: int = 10000
    node_reader_port: int = 10001
    node_sidecar_port: int = 10002

    # Only for testing proposes
    writer_port_map: Dict[int, int] = {}
    reader_port_map: Dict[int, int] = {}
    sidecar_port_map: Dict[int, int] = {}


settings = Settings()
