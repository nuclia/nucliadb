STORAGE = {
    "resources": {},
    "binaries": {},
}


def get_resources(kbid: str):
    return STORAGE["resources"].get(kbid, [])


def get_binaries(kbid: str):
    return STORAGE["binaries"].get(kbid, {}).items()


def store_resource(kbid: str, resource):
    STORAGE["resources"].setdefault(kbid, []).append(resource)


def store_binary(kbid, rid, data):
    STORAGE["binaries"].setdefault(kbid, {})[rid] = data


def clear_resources(kbid: str):
    STORAGE["resources"].pop(kbid, None)


def clear_binaries(kbid: str):
    STORAGE["binaries"].pop(kbid, None)
