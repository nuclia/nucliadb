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

import os
from importlib import resources

from grpc_tools import protoc


def pdm_build_initialize(context):
    build_dir = context.ensure_build_dir()

    well_known_path = resources.files("grpc_tools") / "_proto"

    # Compile protos
    for proto, has_grpc in [
        ("nidx_protos/nidx.proto", True),
        ("nidx_protos/nodereader.proto", False),
        ("nidx_protos/nodewriter.proto", False),
        ("nidx_protos/noderesources.proto", False),
    ]:
        command = [
            "grpc_tools.protoc",
            "--proto_path=..",
            "--proto_path=../../",
            f"--proto_path={well_known_path}",
            f"--python_out={build_dir}",
            f"--mypy_out={build_dir}",
            proto,
        ]
        if has_grpc:
            command.append(f"--grpc_python_out={build_dir}")

        if protoc.main(command) != 0:
            raise Exception("error: {} failed".format(command))

    # Create py.typed to enable type checking
    open(f"{build_dir}/nidx_protos/py.typed", "w")
