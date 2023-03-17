import hashlib
from base64 import b64encode
from os.path import dirname


def load_file_as_FileB64_payload(f: str, content_type: str) -> dict:
    file_location = f"{dirname(__file__)}/{f}"
    filename = f.split("/")[-1]
    data = b64encode(open(file_location, "rb").read())

    return {
        "filename": filename,
        "content_type": content_type,
        "payload": data.decode("utf-8"),
        "md5": hashlib.md5(data).hexdigest(),
    }
