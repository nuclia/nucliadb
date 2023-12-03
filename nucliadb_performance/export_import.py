import argparse
import os
import time
from dataclasses import dataclass

import requests
from tqdm import tqdm

from nucliadb_models.resource import ReleaseChannel
from nucliadb_sdk import NucliaSDK
from nucliadb_sdk.v2.exceptions import NotFoundError


@dataclass
class NucliaDB:
    reader: NucliaSDK
    writer: NucliaSDK


LOCAL_API = "http://localhost:8080/api"
CLUSTER_API = "http://{service}.nucliadb.svc.cluster.local:8080/api"

LOCAL_NUCLIADB = True
if LOCAL_NUCLIADB:
    READER_API = WRITER_API = LOCAL_API
else:
    READER_API = CLUSTER_API.format(service="reader")
    WRITER_API = CLUSTER_API.format(service="writer")

MB = 1024 * 1024
CHUNK_SIZE = 10 * MB


ndb = NucliaDB(
    reader=NucliaSDK(url=READER_API, headers={"X-Nucliadb-Roles": "READER"}),
    writer=NucliaSDK(url=WRITER_API, headers={"X-Nucliadb-Roles": "WRITER;MANAGER"}),
)

class ImportedKBAlreadyExists(Exception):
    pass


def get_kb(ndb, slug_or_kbid) -> str:
    try:
        kbid = ndb.reader.get_knowledge_box_by_slug(slug=slug_or_kbid).uuid
    except NotFoundError:
        ndb.reader.get_knowledge_box(kbid=slug_or_kbid)
        kbid = slug_or_kbid
    return kbid


def create_kb(ndb, slug_or_kbid, release_channel=None) -> str:
    lower_slug_or_kbid = slug_or_kbid.lower()
    try:
        kbid = get_kb(ndb, lower_slug_or_kbid)
    except NotFoundError:
        kbid = ndb.writer.create_knowledge_box(
            slug=lower_slug_or_kbid, release_channel=release_channel
        ).uuid
    else:
        raise ImportedKBAlreadyExists(kbid)
    return kbid


def import_kb(*, uri, kb, release_channel=None):
    try:
        kbid = create_kb(ndb, kb, release_channel=release_channel)
    except ImportedKBAlreadyExists:
        print(f"Skipping import, as the Knowledge Box {kb} already exists")
        return
    print(f"Importing from {uri} to kb={kb}")

    import_id = ndb.writer.start_import(
        kbid=kbid, content=read_import_stream(uri)
    ).import_id

    print(f"Started import task. Import id: {import_id}")
    wait_until_finished(ndb, kbid, "import", import_id)

    print(f"Import finished!")


def export_kb(*, uri, kb):
    kbid = get_kb(ndb, kb)
    export_id = ndb.writer.start_export(kbid=kbid).export_id

    print(f"Starting export for {kb}. Export id: {export_id}")
    wait_until_finished(ndb, kbid, "export", export_id)

    print(f"Downloading export at {uri}")
    export_generator = ndb.reader.download_export(kbid=kbid, export_id=export_id)
    save_export_stream(uri, export_generator)

    print(f"Export finished!")


def get_status(ndb, kbid, task_type, task_id):
    if task_type == "import":
        return ndb.reader.import_status(kbid=kbid, import_id=task_id)
    elif task_type == "export":
        return ndb.reader.export_status(kbid=kbid, export_id=task_id)
    else:
        raise ValueError(f"Unknown task type {task_type}")


def wait_until_finished(ndb, kbid, task_type, task_id, wait_interval=2):
    status = get_status(ndb, kbid, task_type, task_id)
    with tqdm(
        total=status.total, desc=f"Waiting for {task_type} {task_id} to finish"
    ) as progress_bar:
        while status.status != "finished":
            progress_bar.update(status.processed - progress_bar.n)
            assert status.status != "error"
            time.sleep(wait_interval)
            status = get_status(ndb, kbid, task_type, task_id)


def save_export_stream(uri, export_generator):
    tqdm_kwargs = dict(
        desc="Downloading export from NucliaDB",
        unit="iB",
        unit_scale=True,
    )
    stream_with_progress = progressify(
        export_generator(chunk_size=CHUNK_SIZE * 10), **tqdm_kwargs
    )
    if uri.startswith("http"):
        save_export_to_url(uri, stream_with_progress)
    else:
        save_export_to_file(uri, stream_with_progress)


def save_export_to_file(export_path, export_generator):
    with open(export_path, "wb") as f:
        for chunk in export_generator:
            f.write(chunk)


def save_export_to_url(uri, export_generator):
    response = requests.put(uri, data=export_generator)
    response.raise_for_status()


def read_import_stream(uri):
    tqdm_kwargs = dict(
        desc="Uploading export to NucliaDB",
        unit="iB",
        unit_scale=True,
    )
    path_for_uri = get_export_file_from_url(uri)
    path_for_uri_exists = os.path.exists(path_for_uri)
    if uri.startswith("http") and not path_for_uri_exists:
        stream = read_from_url
    else:
        if path_for_uri_exists:
            print(f"Using local file {path_for_uri} instead of URL {uri}")
            uri = path_for_uri
        stream = read_from_file
        tqdm_kwargs["total"] = os.path.getsize(uri)
    for chunk in progressify(stream(uri), **tqdm_kwargs):
        yield chunk


def read_from_file(path):
    with open(path, mode="rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield chunk


def read_from_url(uri):
    """
    Read from a URL using requests, but also save the read chunks to disk.
    """
    path_for_uri = get_export_file_from_url(uri)
    with open(path_for_uri, mode="wb") as f:
        with requests.get(uri, stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                yield chunk
                # Save to disk too
                f.write(chunk)


def get_export_file_from_url(uri):
    export_name = uri.split("/")[-1]
    return f"./{export_name}"


def progressify(func, **tqdm_kwargs):
    with tqdm(**tqdm_kwargs) as progress_bar:
        for chunk in func:
            progress_bar.update(len(chunk))
            yield chunk


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["export", "import"], required=True)
    parser.add_argument("--uri", type=str, required=True)
    parser.add_argument("--kb", type=str, required=True)
    parser.add_argument(
        "--release_channel",
        type=str,
        choices=[v.value.lower() for v in ReleaseChannel],
        default=ReleaseChannel.STABLE.value,
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()
    if args.action == "export":
        export_kb(uri=args.uri, kb=args.kb)
    elif args.action == "import":
        release_channel = ReleaseChannel(args.release_channel.upper())
        import_kb(uri=args.uri, kb=args.kb, release_channel=release_channel)
    else:
        raise ValueError(f"Unknown action {args.action}")


if __name__ == "__main__":
    main()
