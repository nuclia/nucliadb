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

import base64
from functools import partial
from io import BytesIO
from nucliadb_protos.train_pb2 import TrainSet
from datasets.filesystems.s3filesystem import S3FileSystem
import pytest
import asyncio
import requests
from httpx import AsyncClient
from botocore.session import Session
from aiobotocore.session import AioSession


@pytest.mark.asyncio
async def test_list_bucket(train_rest_api, knowledgebox):
    s3 = fs.S3FileSystem(
        region="nuclia",
        scheme="http",
        endpoint_override=train_rest_api,
        access_key="READER",
        secret_key="XXXX",
    )
    train = TrainSet()
    train.kbid = knowledgebox
    trainset = base64.b64encode(train.SerializeToString()).decode().replace("=", "-")
    file_selector = fs.FileSelector(trainset, recursive=True)
    loop = asyncio.get_running_loop()
    partitions = await loop.run_in_executor(None, s3.get_file_info, file_selector)

    assert len(partitions) == 1

    s3_fs = S3FileSystem(
        key="READER",
        secret="XXXX",
        asynchronous=True,
        # session=AioSession(),
        loop=asyncio.get_running_loop(),
        client_kwargs={"endpoint_url": "http://" + train_rest_api},
    )
    partitions = await s3_fs._ls(trainset)
    assert len(partitions) == 1

    # file_data = await loop.run_in_executor(None, s3.open_input_stream, partitions[0])

    # file_readed2 = await loop.run_in_executor(None, file_data.readall)

    # import pdb

    # pdb.set_trace()

    dataset_for_executor = partial(ds.dataset, partitions, format="ipc", filesystem=s3)
    dataset = await loop.run_in_executor(None, dataset_for_executor)

    assert len(dataset.files) == 1

    # async with AsyncClient() as client:
    #     data = await client.get(
    #         "http://" + train_rest_api + "/" + partitions[0],
    #         headers={"authorization": "XX YY=READER/"},
    #     )

    #     buffer = BytesIO()
    #     buffer.write(data.content)
    #     import pdb

    #     pdb.set_trace()
    #     buffer.seek(0)
    #     with pa.ipc.open_stream(buffer) as reader:
    #         schema = reader.schema
    #         batches = [b for b in reader]
    #         import pdb

    #         pdb.set_trace()

    #     # buffer.seek(0)
    # import pdb

    # pdb.set_trace()
    ll = []

    def generator(dataset, ll):
        for batch in dataset.to_batches(
            columns=["n_legs"], filter=~ds.field("n_legs").is_null()
        ):
            ll.append(batch)

    await loop.run_in_executor(None, generator, dataset, ll)
    import pdb

    pdb.set_trace()
