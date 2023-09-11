from shared import Resource

STORAGE = {
    "resources": {
        "kb1": [Resource(id="rid1", data="D" * 1)],
    },
    "binaries": {
        "kb1": {"rid1": b"B" * 5},
    },
}


def get_resources(kbid: str):
    return STORAGE["resources"].get(kbid, [])


def get_binaries(kbid: str):
    return STORAGE["binaries"].get(kbid, {})


def store_resource(kbid: str, resource):
    STORAGE["resources"].setdefault(kbid, []).append(resource)


def store_binary(kbid, rid, data):
    STORAGE["binaries"].setdefault(kbid, {})[rid] = data


def clear_resources(kbid: str):
    STORAGE["resources"].pop(kbid, None)


def clear_binaries(kbid: str):
    STORAGE["binaries"].pop(kbid, None)
