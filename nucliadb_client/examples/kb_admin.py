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

from nucliadb_client.client import NucliaDBClient


class KnowledgeBoxAdmin:
    def __init__(
        self, host, kbid, grpc=8030, http=8080, reader_host=None, writer_host=None
    ):
        self.kbid = kbid
        self.client = NucliaDBClient(
            host=host,
            grpc=grpc,
            http=http,
            reader_host=reader_host,
            writer_host=writer_host,
        )

    def reprocess(self):
        kb = self.client.get_kb(kbid=self.kbid)
        for index, resource in enumerate(kb.iter_resources()):
            print(f"{index}: Reprocessing: ", resource.rid)
            resource.reprocess()

    def reindex(self):
        kb = self.client.get_kb(kbid=self.kbid)
        for index, resource in enumerate(kb.iter_resources()):
            print(f"{index}: Reindexing: ", resource.rid)
            resource.reindex()


def main(args):
    if args.host:
        host_args = dict(host=args.host)
    else:
        host_args = dict(
            host="",
            writer_host=args.writer_host or "writer.nucliadb.svc.cluster.local",
            reader_host=args.reader_host or "reader.nucliadb.svc.cluster.local",
        )

    admin = KnowledgeBoxAdmin(kbid=args.kb, **host_args, http=args.http, grpc=args.grpc)

    if args.action == "reprocess":
        admin.reprocess()

    elif args.action == "reindex":
        admin.reindex()

    else:
        raise ValueError(f"Invalid action {args.action}")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Tool to perform admin tasks on a KB")
    parser.add_argument(
        "--host",
        dest="host",
        default=None,
        help="External api host e.g: europe-1.stashify.cloud",
    )
    parser.add_argument(
        "--writer-host",
        dest="writer_host",
        default=None,
        help="internal writer api host",
    )
    parser.add_argument(
        "--reader-host",
        dest="reader_host",
        default=None,
        help="internal reader api host",
    )
    parser.add_argument("--kb", dest="kb", required=True, help="KB uuid")
    parser.add_argument("--action", required=True, choices=["reprocess", "reindex"])
    parser.add_argument("--grpc", dest="grpc", default=8030)

    parser.add_argument(
        "--http",
        dest="http",
        default=8080,
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
