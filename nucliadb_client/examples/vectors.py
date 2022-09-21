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
import functools
from time import perf_counter

import aiofiles
import numpy as np
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.utils_pb2 import Vector

from nucliadb_client.client import NucliaDBClient
from nucliadb_client.knowledgebox import KnowledgeBox
from nucliadb_client.resource import Resource


def get_docs(n, n_dim, kb):
    with open("cache-vectors.nucliadb", "w+") as pblist:
        for i in range(n):
            if i % 50 == 0:
                print(f"{i}")
            r = Resource(rid=str(i), kb=kb)
            vector = Vector()
            vector.vector.extend(np.random.rand(n_dim))
            r.add_vectors("vectors", FieldType.TEXT, [vector])
            pblist.write(base64.b64encode(r.serialize(processor=False)).decode() + "/n")


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        res = func(*args, **kwargs)
        return (perf_counter() - start, res)

    return wrapper


n_query = 1
D = 128


async def upload(kb: KnowledgeBox):
    await kb.init_async_grpc()
    pending = []
    i = 0
    async with aiofiles.open("cache-vectors.nucliadb", "r") as pblist:
        for pbline in await pblist.readlines():
            i += 1
            resource = Resource(rid=None, kb=kb)
            resource.parse(base64.b64decode(pbline.strip()))
            pending.append(resource.commit(processor=False))
            if len(pending) % 100 == 0:
                await asyncio.gather(*pending)
                print("{i}")
                pending = []

        if len(pending):
            await asyncio.gather(*pending)
            print("{i}")


@timer
def create(docs):
    asyncio.run(upload(docs))


if __name__ == "__main__":
    # use default ``xml.sax.expatreader``

    parser = argparse.ArgumentParser(description="Ingest stackoverflow")

    parser.add_argument(
        "--compute",
        dest="compute",
    )

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

    args = parser.parse_args()
    client = NucliaDBClient(
        host=args.host, grpc=args.grpc, http=args.http, train=args.train
    )
    kb = client.get_kb(slug="vectors_single")
    if kb is None:
        kb = client.create_kb(slug="vectors_single", title="Vectors test")

    if args.compute:
        get_docs(1_000_000, D, kb)
    print(f"indexing 1000000 docs ...")
    create_time, _ = create()
    print(f"time {create_time}")

    # print(f'reading 1000000 docs ...')
    # read_time, _ = read(
    #     da,
    #     random.sample([d.id for d in docs], n_query),
    # )

    # print(f'updating {n_query} docs ...')
    # update_time, _ = update(da, docs_to_update)

    # print(f'deleting {n_query} docs ...')
    # delete_time, _ = delete(da, [d.id for d in docs_to_delete])
    # docs_to_delete = random.sample(docs, n_query)
    # docs_to_update = random.sample(docs, n_query)

    # vector_queries = [np.random.rand(n_query, D) for _ in range(1)]
