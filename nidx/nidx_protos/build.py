import os

from grpc_tools import protoc


def pdm_build_initialize(context):
    build_dir = context.ensure_build_dir()
    python_dir = f"{build_dir}/nidx_protos"
    try:
        os.mkdir(python_dir)
    except FileExistsError:
        pass

    # Compile protos
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

    # Create py.typed to enable type checking
    open(f"{python_dir}/py.typed", "w")
