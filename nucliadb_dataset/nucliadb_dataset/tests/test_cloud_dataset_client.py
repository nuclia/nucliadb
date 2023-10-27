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

import os

from nucliadb_dataset.dataset import NucliaCloudDataset


def test_cloud_datasets_gcs(gcs_with_partitions, gcs, temp_folder):
    dataset_client = NucliaCloudDataset(
        base_path=temp_folder,
        remote_storage={
            "storage_type": "gcs",
            "bucket": "test_cloud_datasets",
            "key": "datasets/123456",
            "settings": {
                "base64_creds": None,
                "location": "europe-west4",
                "project": "project",
                "endpoint_url": gcs,
            },
        },
    )

    partitions = dataset_client.get_partitions()
    assert set(partitions) == {"partition1", "partition2"}

    local = dataset_client.read_all_partitions()
    assert len(local) == 2

    os.stat(local[0]).st_size == 10 * 1024 * 1024
    os.stat(local[1]).st_size == 10 * 1024 * 1024


def test_cloud_datasets_s3(s3_with_partitions, s3, temp_folder):
    dataset_client = NucliaCloudDataset(
        base_path=temp_folder,
        remote_storage={
            "storage_type": "s3",
            "bucket": "testclouddatasets",
            "key": "datasets/123456",
            "settings": {
                "client_id": "",
                "client_secret": "",
                "ssl": False,
                "verify_ssl": False,
                "endpoint": s3,
                "region_name": None,
            },
        },
    )

    partitions = dataset_client.get_partitions()
    assert set(partitions) == {"partition1", "partition2"}

    local = dataset_client.read_all_partitions()
    assert len(local) == 2

    os.stat(local[0]).st_size == 10 * 1024 * 1024
    os.stat(local[1]).st_size == 10 * 1024 * 1024
