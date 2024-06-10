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

import argparse
import os
import time
from typing import Any

import requests
from tqdm import tqdm

from nucliadb_performance.utils.nucliadb import get_kbid, get_nucliadb_client

MB = 1024 * 1024
CHUNK_SIZE = 10 * MB


def import_kb(ndb, *, uri, kb):
    kbid = get_kbid(ndb, kb)
    print(f"Importing from {uri} to kb={kb}")

    import_id = ndb.writer.start_import(kbid=kbid, content=read_import_stream(uri)).import_id

    print(f"Started import task. Import id: {import_id}")
    wait_until_finished(ndb, kbid, "import", import_id)

    print(f"Import finished!")


def export_kb(ndb, *, uri, kb):
    kbid = get_kbid(ndb, kb)
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
    with tqdm(total=status.total, desc=f"Waiting for {task_type} {task_id} to finish") as progress_bar:
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
    stream_with_progress = progressify(export_generator(chunk_size=CHUNK_SIZE * 10), **tqdm_kwargs)
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
    tqdm_kwargs: dict[str, Any] = dict(
        desc="Uploading export to NucliaDB",
        unit="iB",
        unit_scale=True,
    )
    if uri.startswith("http"):
        stream = read_from_url
        resp = requests.head(uri)
        resp.raise_for_status()
        tqdm_kwargs["total"] = int(resp.headers["Content-Length"])
    else:
        stream = read_from_file
        if not os.path.exists(uri):
            raise ValueError(f"File {uri} does not exist")
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
    with requests.get(uri, stream=True) as response:
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            yield chunk


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
        "--cluster",
        action="store_true",
        help="Use nucliadb's k8s cluster services instead of localhost",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()
    ndb = get_nucliadb_client(local=not args.cluster)
    if args.action == "export":
        export_kb(ndb, uri=args.uri, kb=args.kb)
    elif args.action == "import":
        import_kb(ndb, uri=args.uri, kb=args.kb)
    else:
        raise ValueError(f"Unknown action {args.action}")


if __name__ == "__main__":
    main()
