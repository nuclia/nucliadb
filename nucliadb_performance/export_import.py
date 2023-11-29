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


API = "http://localhost:8080/api"
CHUNK_SIZE = 1024 * 1025 * 5


ndb = NucliaDB(
    reader=NucliaSDK(url=API, headers={"X-Nucliadb-Roles": "READER"}),
    writer=NucliaSDK(url=API, headers={"X-Nucliadb-Roles": "WRITER;MANAGER"}),
)


def get_or_create_kb(ndb, slug, release_channel=None) -> str:
    try:
        kbid = ndb.reader.get_knowledge_box_by_slug(slug=slug).uuid
    except NotFoundError:
        kbid = ndb.writer.create_knowledge_box(
            slug=slug, release_channel=release_channel
        ).uuid
    return kbid


def import_kb(*, uri, slug, release_channel=None):
    kbid = get_or_create_kb(ndb, slug, release_channel=release_channel)
    print(f"Importing from {uri} to kb={slug}")

    import_id = ndb.writer.start_import(
        kbid=kbid, content=read_import_stream(uri)
    ).import_id
    print(f"Started import task. Import id: {import_id}")

    print("Waiting for the data to be imported")
    status = ndb.reader.import_status(kbid=kbid, import_id=import_id)
    while status.status != "finished":
        print(f"Status: {status.status} {status.processed}/{status.total}")
        assert status.status != "error"
        time.sleep(2)
        status = ndb.reader.import_status(kbid=kbid, import_id=import_id)
    print(f"Import finished!")


def export_kb(*, uri, slug):
    kbid = ndb.reader.get_knowledge_box_by_slug(slug=slug).uuid
    export_id = ndb.writer.start_export(kbid=kbid).export_id

    print(f"Starting export for {slug}. Export id: {export_id}")
    status = ndb.reader.export_status(kbid=kbid, export_id=export_id)
    while status.status != "finished":
        print(f"Status: {status.status} {status.processed}/{status.total}")
        assert status.status != "error"
        time.sleep(2)
        status = ndb.reader.export_status(kbid=kbid, export_id=export_id)

    print(f"Downloading export at {uri}")
    export_generator = ndb.reader.download_export(kbid=kbid, export_id=export_id)
    save_export_stream(uri, export_generator)


def save_export_stream(uri, export_generator):
    tqdm_kwargs = dict(
        desc="Downloading export from NucliaDB",
        unit="iB",
        unit_scale=True,
    )
    stream_with_progress = progressify(export_generator, **tqdm_kwargs)
    if uri.startswith("http"):
        save_export_to_url(uri, stream_with_progress)
    else:
        save_export_to_file(uri, stream_with_progress)


def save_export_to_file(export_path, export_generator):
    with open(export_path, "wb") as f:
        for chunk in export_generator(chunk_size=CHUNK_SIZE * 10):
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
    if uri.startswith("http"):
        stream = read_from_url
    else:
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
    response = requests.get(uri, stream=True)
    response.raise_for_status()
    return response.iter_content(chunk_size=CHUNK_SIZE)


def progressify(func, **tqdm_kwargs):
    with tqdm(**tqdm_kwargs) as progress_bar:
        for chunk in func:
            progress_bar.update(len(chunk))
            yield chunk


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["export", "import"])
    parser.add_argument("--uri", type=str)
    parser.add_argument("--kb", type=str)
    parser.add_argument(
        "--release_channel",
        type=str,
        choices=[v.value for v in ReleaseChannel],
        default=ReleaseChannel.STABLE.value,
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()
    if args.action == "export":
        export_kb(uri=args.uri, slug=args.kb)
    elif args.action == "import":
        release_channel = ReleaseChannel(args.release_channel)
        import_kb(uri=args.uri, slug=args.kb, release_channel=release_channel)


if __name__ == "__main__":
    main()
