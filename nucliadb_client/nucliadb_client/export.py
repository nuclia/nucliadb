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
    parser = argparse.ArgumentParser(description="Export KB")

    parser.add_argument(
        "--host",
        dest="host",
    )

    parser.add_argument("--grpc", dest="grpc", description="Ingest gRPC api port")

    parser.add_argument("--http", dest="http", description="HTTP api port")

    parser.add_argument(
        "--slug",
        dest="slug",
    )

    parser.add_argument(
        "--dump",
        dest="dump",
    )

    parser.add_argument(
        "--reader_host",
        dest="reader_host",
        description="Hostname of nucliadb's reader service",
    )

    parser.add_argument(
        "--writer_host",
        dest="writer_host",
        description="Hostname of nucliadb's writer service",
    )

    parser.add_argument(
        "--grpc_host",
        dest="grpc_host",
        description="Hostname of nucliadb's ingest gRPC service",
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
    kb = client.get_kb(slug=args.slug)
    if kb is None:
        raise KeyError("KB not found")
    asyncio.run(kb.export(args.dump))
