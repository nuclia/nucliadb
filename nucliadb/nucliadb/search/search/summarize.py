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
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.common.maindb.utils import get_driver
from nucliadb.search.utilities import get_predict
from nucliadb_models.common import FIELD_TYPES_MAP
from nucliadb_models.search import (
    SummarizedResponse,
    SummarizeModel,
    SummarizeRequest,
    SummarizeResourceModel,
)
from nucliadb_utils.utilities import get_storage


async def summarize(kbid: str, request: SummarizeRequest) -> SummarizedResponse:
    predict_request = SummarizeModel()

    driver = get_driver()
    storage = await get_storage()
    rdm = ResourcesDataManager(driver, storage)

    for resource_uuid in set(request.resources):
        resource = await rdm.get_resource(kbid, resource_uuid)
        if resource is None:
            continue

        predict_request.resources[resource_uuid] = SummarizeResourceModel()
        fields = await resource.get_fields(force=True)
        for (field_type, field_id), field in fields.items():
            field_extracted_text = await field.get_extracted_text(force=True)
            if field_extracted_text is None:
                continue

            field_type_str = FIELD_TYPES_MAP[field_type].value
            field_key = f"{field_type_str}/{field_id}"
            predict_request.resources[resource_uuid].fields[
                field_key
            ] = field_extracted_text.text

    predict = get_predict()
    return await predict.summarize(kbid, predict_request)
