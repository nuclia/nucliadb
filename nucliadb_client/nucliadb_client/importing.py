# -*- coding: utf-8 -*-
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
import asyncio

from nucliadb_client.client import NucliaDBClient


def parse_arguments():
    parser = argparse.ArgumentParser(description="Import KB")
    parser.add_argument(
        "--host", dest="host", default="ingest.nucliadb.svc.cluster.local"
    )

    parser.add_argument("--grpc", dest="grpc", default="8030")

    parser.add_argument("--http", dest="http", default="8080")

    parser.add_argument(
        "--kbid",
        dest="kbid",
    )

    parser.add_argument(
        "--dump",
        dest="dump",
    )

    parser.add_argument(
        "--reader_host", dest="reader_host", default="reader.nucliadb.svc.cluster.local"
    )

    parser.add_argument(
        "--writer_host", dest="writer_host", default="writer.nucliadb.svc.cluster.local"
    )

    parser.add_argument(
        "--grpc_host", dest="grpc_host", default="ingest.nucliadb.svc.cluster.local"
    )

    return parser.parse_args()


def run():
    args = parse_arguments()
    client = NucliaDBClient(
        host=args.host,
        grpc=args.grpc,
        http=args.http,
        writer_host=args.writer_host,
        reader_host=args.reader_host,
        grpc_host=args.grpc_host,
    )
    kb = client.get_kb(kbid=args.kbid)
    if kb is not None:
        raise KeyError(f"KB could not be found")
    asyncio.run(client.import_kb(kbid=args.kbid, location=args.dump))


if __name__ == "__main__":
    run()
