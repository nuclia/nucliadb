import os
from importlib import resources

from grpc_tools import protoc


def pdm_build_initialize(context):
    build_dir = context.ensure_build_dir()
    python_dir = f"{build_dir}/nidx_protos"
    try:
        os.mkdir(python_dir)
    except FileExistsError:
        pass

    well_known_path = resources.files("grpc_tools") / "_proto"

    # Compile protos
    for proto in [
        "src/nidx.proto",
    ]:
        command = [
            "grpc_tools.protoc",
            "--proto_path=src",
            "--proto_path=../../",
            f"--proto_path={well_known_path}",
            f"--python_out={python_dir}",
            f"--pyi_out={python_dir}",
            f"--grpc_python_out={python_dir}",
            proto,
        ]
        if protoc.main(command) != 0:
            raise Exception("error: {} failed".format(command))

    # Create py.typed to enable type checking
    open(f"{python_dir}/py.typed", "w")


def get_version():
    return open("../../VERSION").read()
