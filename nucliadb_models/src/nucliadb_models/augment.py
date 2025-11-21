# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pydantic import BaseModel

from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import Image, ResourceProperties, SearchParamDefaults

ParagraphId = str


class AugmentParagraph(BaseModel):
    id: ParagraphId
    # TODO: paragraph metadata


class AugmentResources(BaseModel):
    given: list[str]

    show: list[ResourceProperties] = SearchParamDefaults.show.to_pydantic_field()
    extracted: list[ExtractedDataTypeName] = SearchParamDefaults.extracted.to_pydantic_field()
    field_type_filter: list[FieldTypeName] = SearchParamDefaults.field_type_filter.to_pydantic_field()
    # TODO: field name filter, da field prefix filter


class AugmentParagraphs(BaseModel):
    given: list[AugmentParagraph]

    text: bool = True

    neighbours_before: int = 0
    neighbours_after: int = 0

    # paragraph extracted from an image, return an image
    source_image: bool = False

    # paragraph extracted from a table, return table image
    table_image: bool = False

    # return page_preview instead of table image if table image enabled
    table_prefers_page_preview: bool = False

    # paragraph from a page, return page preview image
    page_preview_image: bool = False


class AugmentRequest(BaseModel):
    resources: AugmentResources | None = None
    paragraphs: AugmentParagraphs | None = None


class AugmentedParagraph(BaseModel):
    text: str | None = None

    neighbours_before: dict[ParagraphId, str] | None = None
    neighbours_after: dict[ParagraphId, str] | None = None

    image: Image | None = None


class AugmentedField(BaseModel):
    page_preview_image: Image | None = None


class AugmentedResource(Resource):
    def updated_from(self, origin: Resource):
        for key in origin.model_fields.keys():
            self.__setattr__(key, getattr(origin, key))


class AugmentResponse(BaseModel):
    resources: dict[str, AugmentedResource]
    paragraphs: dict[str, AugmentedParagraph]
