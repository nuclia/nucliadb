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
#
from nidx_protos.noderesources_pb2 import Resource as IndexMessage

from nucliadb.common.catalog.interface import CatalogResourceData
from nucliadb.ingest.orm.resource import Resource


def build_catalog_resource_data(resource: Resource, index_message: IndexMessage) -> CatalogResourceData:
    if resource.basic is None:
        raise ValueError("Cannot index into the catalog a resource without basic metadata ")

    created_at = resource.basic.created.ToDatetime()
    modified_at = resource.basic.modified.ToDatetime()
    if modified_at < created_at:
        modified_at = created_at

    # Do not index canceled labels
    cancelled_labels = {
        f"/l/{clf.labelset}/{clf.label}"
        for clf in resource.basic.usermetadata.classifications
        if clf.cancelled_by_user
    }

    # Labels from the resource and classification labels from each field
    labels = {label for label in index_message.labels}
    for classification in resource.basic.computedmetadata.field_classifications:
        for clf in classification.classifications:
            label = f"/l/{clf.labelset}/{clf.label}"
            if label not in cancelled_labels:
                labels.add(label)

    return CatalogResourceData(
        title=resource.basic.title,
        created_at=created_at,
        modified_at=modified_at,
        labels=list(labels),
        slug=resource.basic.slug,
    )
