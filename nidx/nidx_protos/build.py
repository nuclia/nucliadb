import os

from grpc_tools import protoc


def pdm_build_initialize(context):
    build_dir = context.ensure_build_dir()
    python_dir = f"{build_dir}/nidx_protos"
    try:
        os.mkdir(python_dir)
    except FileExistsError:
        pass

    for proto in [
        "src/nidx.proto",
    ]:
        command = [
            "grpc_tools.protoc",
            "--proto_path={}".format("src"),
            "--proto_path={}".format("../../"),
            "--python_out={}".format(python_dir),
            "--pyi_out={}".format(python_dir),
            "--grpc_python_out={}".format(python_dir),
        ] + [proto]
        if protoc.main(command) != 0:
            raise Exception("error: {} failed".format(command))
