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

import pytest

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.filter_expression import facet_from_filter, filter_from_facet
from nucliadb_models.common import Paragraph
from nucliadb_models.filters import (
    And,
    CatalogFilterExpression,
    Entity,
    FacetFilter,
    FieldMimetype,
    FilterExpression,
    Generated,
    Kind,
    Label,
    Language,
    Or,
    OriginCollaborator,
    OriginMetadata,
    OriginPath,
    OriginSource,
    OriginTag,
    ResourceMimetype,
    Status,
)
from nucliadb_models.metadata import ResourceProcessingStatus


@pytest.mark.parametrize(
    "facet,expr",
    [
        ("/t/mytag", OriginTag(tag="mytag")),
        ("/l/labelset", Label(labelset="labelset")),
        ("/l/labelset/label", Label(labelset="labelset", label="label")),
        ("/l/labelset/label/...", Label(labelset="labelset", label="label/...")),
        ("/n/i/image", ResourceMimetype(type="image")),
        ("/n/i/image/png", ResourceMimetype(type="image", subtype="png")),
        ("/n/i/image/png/...", ResourceMimetype(type="image", subtype="png/...")),
        ("/mt/image", FieldMimetype(type="image")),
        ("/mt/image/png", FieldMimetype(type="image", subtype="png")),
        ("/mt/image/png/...", FieldMimetype(type="image", subtype="png/...")),
        ("/e/subtype", Entity(subtype="subtype")),
        ("/e/subtype/value", Entity(subtype="subtype", value="value")),
        ("/e/subtype/value/...", Entity(subtype="subtype", value="value/...")),
        ("/e/subtype/value/...", Entity(subtype="subtype", value="value/...")),
        ("/s/p/en", Language(language="en", only_primary=True)),
        ("/s/s/en", Language(language="en", only_primary=False)),
        ("/m/field/value", OriginMetadata(field="field", value="value")),
        ("/m/field/value/...", OriginMetadata(field="field", value="value/...")),
        ("/p/path/to/my/file", OriginPath(prefix="path/to/my/file")),
        ("/g/da", Generated(by="data-augmentation")),
        ("/g/da/mytask", Generated(by="data-augmentation", da_task="mytask")),
        (f"/k/{Paragraph.TypeParagraph.TEXT.value.lower()}", Kind(kind=Paragraph.TypeParagraph.TEXT)),
        (f"/k/{Paragraph.TypeParagraph.OCR.value.lower()}", Kind(kind=Paragraph.TypeParagraph.OCR)),
        (
            f"/k/{Paragraph.TypeParagraph.INCEPTION.value.lower()}",
            Kind(kind=Paragraph.TypeParagraph.INCEPTION),
        ),
        (
            f"/k/{Paragraph.TypeParagraph.DESCRIPTION.value.lower()}",
            Kind(kind=Paragraph.TypeParagraph.DESCRIPTION),
        ),
        (
            f"/k/{Paragraph.TypeParagraph.TRANSCRIPT.value.lower()}",
            Kind(kind=Paragraph.TypeParagraph.TRANSCRIPT),
        ),
        (f"/k/{Paragraph.TypeParagraph.TITLE.value.lower()}", Kind(kind=Paragraph.TypeParagraph.TITLE)),
        (f"/k/{Paragraph.TypeParagraph.TABLE.value.lower()}", Kind(kind=Paragraph.TypeParagraph.TABLE)),
        ("/u/o/collaborator", OriginCollaborator(collaborator="collaborator")),
        ("/u/s/source", OriginSource(id="source")),
        (
            f"/n/s/{ResourceProcessingStatus.PENDING.value}",
            Status(status=ResourceProcessingStatus.PENDING),
        ),
        (
            f"/n/s/{ResourceProcessingStatus.PROCESSED.value}",
            Status(status=ResourceProcessingStatus.PROCESSED),
        ),
        (f"/n/s/{ResourceProcessingStatus.ERROR.value}", Status(status=ResourceProcessingStatus.ERROR)),
        (f"/n/s/{ResourceProcessingStatus.EMPTY.value}", Status(status=ResourceProcessingStatus.EMPTY)),
        (
            f"/n/s/{ResourceProcessingStatus.BLOCKED.value}",
            Status(status=ResourceProcessingStatus.BLOCKED),
        ),
        (
            f"/n/s/{ResourceProcessingStatus.EXPIRED.value}",
            Status(status=ResourceProcessingStatus.EXPIRED),
        ),
    ],
)
def test_facet_to_filter_conversions(facet: str, expr: FacetFilter):
    assert facet_from_filter(expr) == facet
    assert filter_from_facet(facet) == expr


@pytest.mark.parametrize("facet", ["/k/invalid", "/n/s/invalid", "/not/valid"])
def test_invalid_filters(facet: str):
    with pytest.raises(InvalidQueryError):
        filter_from_facet(facet)


def test_catalog_from_facets():
    filter_expression = CatalogFilterExpression.from_facets(
        ["/t/mytag", "/l/labelset/label", "/n/i/image/png"]
    )
    assert isinstance(filter_expression.resource, And)
    assert len(filter_expression.resource.operands) == 3
    assert isinstance(filter_expression.resource.operands[0], OriginTag)
    assert filter_expression.resource.operands[0].tag == "mytag"
    assert isinstance(filter_expression.resource.operands[1], Label)
    assert filter_expression.resource.operands[1].labelset == "labelset"
    assert filter_expression.resource.operands[1].label == "label"
    assert isinstance(filter_expression.resource.operands[2], ResourceMimetype)
    assert filter_expression.resource.operands[2].type == "image"
    assert filter_expression.resource.operands[2].subtype == "png"

    # Check single filter
    single_filter = CatalogFilterExpression.from_facets(["/l/labelset"])
    assert isinstance(single_filter.resource, Label)
    assert single_filter.resource.labelset == "labelset"

    # Check OR operator
    or_filter = CatalogFilterExpression.from_facets(["/t/tag1", "/t/tag2"], operator="or")
    assert isinstance(or_filter.resource, Or)
    assert len(or_filter.resource.operands) == 2

    # Check that non-catalog facets are ignored
    with pytest.raises(ValueError):
        CatalogFilterExpression.from_facets(["/k/text"])

    # Check that empty facets raise an error
    with pytest.raises(ValueError):
        CatalogFilterExpression.from_facets([])


def test_filter_expression_from_field_facets():
    filter_expression = FilterExpression.from_field_facets(
        ["/t/mytag", "/l/labelset/label", "/mt/image/png", "/e/PERSON/Alice"]
    )
    assert filter_expression.field is not None
    assert isinstance(filter_expression.field, And)
    assert len(filter_expression.field.operands) == 4
    assert isinstance(filter_expression.field.operands[0], OriginTag)
    assert filter_expression.field.operands[0].tag == "mytag"
    assert isinstance(filter_expression.field.operands[1], Label)
    assert filter_expression.field.operands[1].labelset == "labelset"
    assert filter_expression.field.operands[1].label == "label"
    assert isinstance(filter_expression.field.operands[2], FieldMimetype)
    assert filter_expression.field.operands[2].type == "image"
    assert filter_expression.field.operands[2].subtype == "png"
    assert isinstance(filter_expression.field.operands[3], Entity)
    assert filter_expression.field.operands[3].subtype == "PERSON"
    assert filter_expression.field.operands[3].value == "Alice"

    # Check single filter
    single_filter = FilterExpression.from_field_facets(["/l/labelset"])
    assert isinstance(single_filter.field, Label)
    assert single_filter.field.labelset == "labelset"

    # Check OR operator
    or_filter = FilterExpression.from_field_facets(["/t/tag1", "/t/tag2"], operator="or")
    assert isinstance(or_filter.field, Or)
    assert len(or_filter.field.operands) == 2

    # Check that empty facets raise an error
    with pytest.raises(ValueError):
        FilterExpression.from_field_facets([])
