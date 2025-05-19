# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from importlib import resources

from grpc_tools import protoc


def pdm_build_initialize(context):
    build_dir = context.ensure_build_dir()

    well_known_path = resources.files("grpc_tools") / "_proto"

    # Compile protos
    for proto, has_grpc in [
        ("nucliadb_protos/utils.proto", False),
        ("nucliadb_protos/resources.proto", False),
        ("nucliadb_protos/knowledgebox.proto", False),
        ("nucliadb_protos/audit.proto", False),
        ("nucliadb_protos/backups.proto", True),
        ("nucliadb_protos/writer.proto", True),
        ("nucliadb_protos/train.proto", True),
        ("nucliadb_protos/dataset.proto", False),
        ("nucliadb_protos/migrations.proto", False),
        ("nucliadb_protos/standalone.proto", True),
        ("nucliadb_protos/kb_usage.proto", False),
    ]:
        command = [
            "grpc_tools.protoc",
            "--proto_path=..",
            f"--proto_path={well_known_path}",
            f"--python_out={build_dir}",
            f"--mypy_out={build_dir}",
            proto,
        ]
        if has_grpc:
            command.append(f"--grpc_python_out={build_dir}")
            command.append(f"--mypy_grpc_out={build_dir}")

        if protoc.main(command) != 0:
            raise Exception("error: {} failed".format(command))

    # Create py.typed to enable type checking
    open(f"{build_dir}/nucliadb_protos/py.typed", "w")
