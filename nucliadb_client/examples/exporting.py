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
import base64

import aiofiles

from nucliadb_client.client import NucliaDBClient
from nucliadb_client.knowledgebox import KnowledgeBox


async def exporting(kb: KnowledgeBox, dump: str):
    kb.init_async_grpc()
    async with aiofiles.open(dump, "w+") as dump_file:
        async for bm in kb.export():
            await dump_file.write(
                base64.b64encode(bm.SerializeToString()).decode() + "/n"
            )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Export KB")

    parser.add_argument(
        "--host",
        dest="host",
    )

    parser.add_argument(
        "--grpc",
        dest="grpc",
    )

    parser.add_argument(
        "--http",
        dest="http",
    )

    parser.add_argument(
        "--train",
        dest="train",
    )

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
    )

    parser.add_argument(
        "--writer_host",
        dest="writer_host",
    )

    parser.add_argument(
        "--grpc_host",
        dest="grpc_host",
    )

    args = parser.parse_args()
    client = NucliaDBClient(
        host=args.host,
        grpc=args.grpc,
        http=args.http,
        train=args.train,
        writer_host=args.writer_host,
        reader_host=args.reader_host,
        grpc_host=args.grpc_host,
    )
    kb = client.get_kb(slug=args.slug)
    if kb is None:
        raise KeyError("KB not found")
    asyncio.run(exporting(kb, args.dump))
