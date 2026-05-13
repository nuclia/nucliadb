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


from nidx_protos import nodereader_pb2
from nidx_protos.nodereader_pb2 import FilterExpression as PBFilterExpression
from typing_extensions import assert_never

from nucliadb.common import datamanagers
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb_models.common import Paragraph
from nucliadb_models.filters import (
    And,
    DateCreated,
    DateModified,
    Entity,
    Field,
    FieldFilterExpression,
    FieldMimetype,
    Generated,
    Keyword,
    Kind,
    KVBoolMatch,
    KVExactMatch,
    KVFilterExpression,
    KVRange,
    Label,
    Language,
    Not,
    Or,
    OriginCollaborator,
    OriginMetadata,
    OriginPath,
    OriginSource,
    OriginTag,
    ParagraphFilterExpression,
    Resource,
    ResourceMimetype,
    Status,
)
from nucliadb_models.kv_schemas import KBKVSchemas
from nucliadb_models.metadata import ResourceProcessingStatus

# Filters that end up as a facet
FacetFilter = (
    OriginTag
    | Label
    | ResourceMimetype
    | FieldMimetype
    | Entity
    | Language
    | OriginMetadata
    | OriginPath
    | Generated
    | Kind
    | OriginCollaborator
    | OriginSource
    | Status
)


async def parse_expression(
    expr: FieldFilterExpression | ParagraphFilterExpression,
    kbid: str,
) -> PBFilterExpression:
    f = PBFilterExpression()

    if isinstance(expr, And):
        for op in expr.operands:
            f.bool_and.operands.append(await parse_expression(op, kbid))
    elif isinstance(expr, Or):
        for op in expr.operands:
            f.bool_or.operands.append(await parse_expression(op, kbid))
    elif isinstance(expr, Not):
        f.bool_not.CopyFrom(await parse_expression(expr.operand, kbid))
    elif isinstance(expr, Resource):
        if expr.id:
            f.resource.resource_id = expr.id
        elif expr.slug:
            rid = await datamanagers.atomic.resources.get_resource_uuid_from_slug(
                kbid=kbid, slug=expr.slug
            )
            if rid is None:
                raise InvalidQueryError("slug", f"Cannot find slug {expr.slug}")
            f.resource.resource_id = rid
        else:  # pragma: no cover
            # Cannot happen due to model validation
            raise ValueError("Resource needs id or slug")
    elif isinstance(expr, Field):
        f.field.field_type = expr.type.abbreviation()
        if expr.name:
            f.field.field_id = expr.name
    elif isinstance(expr, Keyword):
        f.keyword.keyword = expr.word
    elif isinstance(expr, DateCreated):
        f.date.field = PBFilterExpression.DateRangeFilter.DateField.CREATED
        if expr.since:
            f.date.since.FromDatetime(expr.since)
        if expr.until:
            f.date.until.FromDatetime(expr.until)
    elif isinstance(expr, DateModified):
        f.date.field = PBFilterExpression.DateRangeFilter.DateField.MODIFIED
        if expr.since:
            f.date.since.FromDatetime(expr.since)
        if expr.until:
            f.date.until.FromDatetime(expr.until)
    elif isinstance(expr, FacetFilter):
        f.facet.facet = facet_from_filter(expr)
    else:
        assert_never(expr)

    return f


def parse_kv_filter_expression(
    expr: KVFilterExpression,
) -> nodereader_pb2.JsonFilterExpression:
    """Convert a KVFilterExpression tree into a JsonFilterExpression proto."""
    if isinstance(expr, And):
        result = nodereader_pb2.JsonFilterExpression()
        result.bool_and.operands.extend([parse_kv_filter_expression(op) for op in expr.operands])
        return result
    elif isinstance(expr, Or):
        result = nodereader_pb2.JsonFilterExpression()
        result.bool_or.operands.extend([parse_kv_filter_expression(op) for op in expr.operands])
        return result
    elif isinstance(expr, Not):
        result = nodereader_pb2.JsonFilterExpression()
        result.bool_not.CopyFrom(parse_kv_filter_expression(expr.operand))
        return result
    elif isinstance(expr, KVExactMatch):
        path = nodereader_pb2.JsonFieldPathFilter(
            field_id=f"k/{expr.field_id}",
            json_path=expr.key,
        )
        path.text = expr.value
        return nodereader_pb2.JsonFilterExpression(path=path)
    elif isinstance(expr, KVRange):
        path = nodereader_pb2.JsonFieldPathFilter(
            field_id=f"k/{expr.field_id}",
            json_path=expr.key,
        )
        # Detect int vs float from the bound values
        is_int = all(
            v is None or (isinstance(v, int) and not isinstance(v, bool)) for v in [expr.gte, expr.lte]
        )
        if is_int:
            if expr.gte is not None:
                path.int_range.lower = int(expr.gte)
            if expr.lte is not None:
                path.int_range.upper = int(expr.lte)
        else:
            if expr.gte is not None:
                path.float_range.lower = expr.gte
            if expr.lte is not None:
                path.float_range.upper = expr.lte
        return nodereader_pb2.JsonFilterExpression(path=path)
    elif isinstance(expr, KVBoolMatch):
        path = nodereader_pb2.JsonFieldPathFilter(
            field_id=f"k/{expr.field_id}",
            json_path=expr.key,
        )
        path.boolean = expr.value
        return nodereader_pb2.JsonFilterExpression(path=path)
    else:
        assert_never(expr)


def _parse_kv_filter_expression(
    expr: KVFilterExpression,
    all_schemas: KBKVSchemas,
    kbid: str,
) -> nodereader_pb2.JsonFilterExpression:
    """Recursive helper that validates a KVFilterExpression tree using pre-fetched schemas."""
    if isinstance(expr, And):
        result = nodereader_pb2.JsonFilterExpression()
        result.bool_and.operands.extend(
            [_parse_kv_filter_expression(op, all_schemas, kbid) for op in expr.operands]
        )
        return result
    elif isinstance(expr, Or):
        result = nodereader_pb2.JsonFilterExpression()
        result.bool_or.operands.extend(
            [_parse_kv_filter_expression(op, all_schemas, kbid) for op in expr.operands]
        )
        return result
    elif isinstance(expr, Not):
        result = nodereader_pb2.JsonFilterExpression()
        result.bool_not.CopyFrom(_parse_kv_filter_expression(expr.operand, all_schemas, kbid))
        return result
    elif isinstance(expr, (KVExactMatch, KVRange, KVBoolMatch)):
        schema = all_schemas.schemas.get(expr.field_id)
        if schema is None:
            raise InvalidQueryError("key_value", f"Unknown key-value schema: '{expr.field_id}'")
        field_map = {f.key: f for f in schema.fields}
        if expr.key not in field_map:
            raise InvalidQueryError(
                "key_value",
                f"Key '{expr.key}' not found in schema '{expr.field_id}'",
            )
        schema_field = field_map[expr.key]
        if isinstance(expr, KVExactMatch) and schema_field.type != "text":
            raise InvalidQueryError(
                "key_value",
                f"Key '{expr.key}' in schema '{expr.field_id}' is of type '{schema_field.type}', "
                f"but 'exact_match' requires type 'text'",
            )
        elif isinstance(expr, KVRange) and schema_field.type not in ("float", "integer"):
            raise InvalidQueryError(
                "key_value",
                f"Key '{expr.key}' in schema '{expr.field_id}' is of type '{schema_field.type}', "
                f"but 'range' requires type 'float' or 'integer'",
            )
        elif isinstance(expr, KVBoolMatch) and schema_field.type != "boolean":
            raise InvalidQueryError(
                "key_value",
                f"Key '{expr.key}' in schema '{expr.field_id}' is of type '{schema_field.type}', "
                f"but 'bool_match' requires type 'boolean'",
            )
        return parse_kv_filter_expression(expr)
    else:
        assert_never(expr)


async def parse_kv_filter_expression_with_validation(
    expr: KVFilterExpression,
    kbid: str,
) -> nodereader_pb2.JsonFilterExpression:
    """Convert a KVFilterExpression tree into a JsonFilterExpression proto,
    validating field_id and key against the KV schema.
    Fetches all schemas once and passes them through the recursive helper."""
    async with datamanagers.with_ro_transaction() as txn:
        all_schemas = await datamanagers.kv_schemas.get_all(txn, kbid=kbid)
    return _parse_kv_filter_expression(expr, all_schemas, kbid)


def facet_from_filter(expr: FacetFilter) -> str:
    if isinstance(expr, OriginTag):
        facet = f"/t/{expr.tag}"
    elif isinstance(expr, Label):
        facet = f"/l/{expr.labelset}"
        if expr.label:
            facet += f"/{expr.label}"
    elif isinstance(expr, ResourceMimetype):
        facet = f"/n/i/{expr.type}"
        if expr.subtype:
            facet += f"/{expr.subtype}"
    elif isinstance(expr, FieldMimetype):
        facet = f"/mt/{expr.type}"
        if expr.subtype:
            facet += f"/{expr.subtype}"
    elif isinstance(expr, Entity):
        facet = f"/e/{expr.subtype}"
        if expr.value:
            facet += f"/{expr.value}"
    elif isinstance(expr, Language):
        if expr.only_primary:
            facet = f"/s/p/{expr.language}"
        else:
            facet = f"/s/s/{expr.language}"
    elif isinstance(expr, OriginMetadata):
        facet = f"/m/{expr.field}"
        if expr.value:
            facet += f"/{expr.value}"
    elif isinstance(expr, OriginPath):
        facet = "/p"
        if expr.prefix:
            # Remove leading/trailing slashes for better compatibility
            clean_prefix = expr.prefix.strip("/")
            facet += f"/{clean_prefix}"
    elif isinstance(expr, Generated):
        facet = "/g/da"
        if expr.da_task:
            facet += f"/{expr.da_task}"
    elif isinstance(expr, Kind):
        facet = f"/k/{expr.kind.lower()}"
    elif isinstance(expr, OriginCollaborator):
        facet = f"/u/o/{expr.collaborator}"
    elif isinstance(expr, OriginSource):
        facet = "/u/s"
        if expr.id:
            facet += f"/{expr.id}"
    elif isinstance(expr, Status):
        facet = f"/n/s/{expr.status.value}"
    else:
        assert_never(expr)

    return facet


def filter_from_facet(facet: str) -> FacetFilter:
    expr: FacetFilter

    if facet.startswith("/t/"):
        value = facet.removeprefix("/t/")
        expr = OriginTag(tag=value)

    elif facet.startswith("/l/"):
        value = facet.removeprefix("/l/")
        parts = value.split("/", maxsplit=1)
        if len(parts) == 1:
            type = parts[0]
            expr = Label(labelset=type)
        else:
            type, subtype = parts
            expr = Label(labelset=type, label=subtype)

    elif facet.startswith("/n/i/"):
        value = facet.removeprefix("/n/i/")
        parts = value.split("/", maxsplit=1)
        if len(parts) == 1:
            type = parts[0]
            expr = ResourceMimetype(type=type)
        else:
            type, subtype = parts
            expr = ResourceMimetype(type=type, subtype=subtype)

    elif facet.startswith("/mt/"):
        value = facet.removeprefix("/mt/")
        parts = value.split("/", maxsplit=1)
        if len(parts) == 1:
            type = parts[0]
            expr = FieldMimetype(type=type)
        else:
            type, subtype = parts
            expr = FieldMimetype(type=type, subtype=subtype)

    elif facet.startswith("/e/"):
        value = facet.removeprefix("/e/")
        parts = value.split("/", maxsplit=1)
        if len(parts) == 1:
            subtype = parts[0]
            expr = Entity(subtype=subtype)
        else:
            subtype, value = parts
            expr = Entity(subtype=subtype, value=value)

    elif facet.startswith("/s/p"):
        value = facet.removeprefix("/s/p/")
        expr = Language(language=value, only_primary=True)

    elif facet.startswith("/s/s"):
        value = facet.removeprefix("/s/s/")
        expr = Language(language=value, only_primary=False)

    elif facet.startswith("/m/"):
        value = facet.removeprefix("/m/")
        parts = value.split("/", maxsplit=1)
        if len(parts) == 1:
            field = parts[0]
            expr = OriginMetadata(field=field)
        else:
            field, value = parts
            expr = OriginMetadata(field=field, value=value)

    elif facet.startswith("/p/"):
        value = facet.removeprefix("/p/")
        expr = OriginPath(prefix=value)

    elif facet.startswith("/g/da"):
        value = facet.removeprefix("/g/da")
        expr = expr = Generated(by="data-augmentation")
        if value.removeprefix("/"):
            expr.da_task = value.removeprefix("/")

    elif facet.startswith("/k/"):
        value = facet.removeprefix("/k/")
        try:
            kind = Paragraph.TypeParagraph(value.upper())
        except ValueError:
            raise InvalidQueryError("filters", f"invalid paragraph kind: {value}")
        expr = Kind(kind=kind)

    elif facet.startswith("/u/o/"):
        value = facet.removeprefix("/u/o/")
        expr = OriginCollaborator(collaborator=value)

    elif facet.startswith("/u/s"):
        value = facet.removeprefix("/u/s")
        expr = OriginSource()
        if value.removeprefix("/"):
            expr.id = value.removeprefix("/")

    elif facet.startswith("/n/s/"):
        value = facet.removeprefix("/n/s/")
        try:
            status = ResourceProcessingStatus(value.upper())
        except ValueError:
            raise InvalidQueryError("filters", f"invalid resource processing status: {value}")
        expr = Status(status=status)

    else:
        raise InvalidQueryError("filters", f"invalid filter: {facet}")

    return expr


def add_and_expression(dest: PBFilterExpression, add: PBFilterExpression):
    dest_expr_type = dest.WhichOneof("expr")
    if dest_expr_type is None:
        dest.CopyFrom(add)
    elif dest_expr_type == "bool_and":
        dest.bool_and.operands.append(add)
    else:
        and_expr = PBFilterExpression()
        and_expr.bool_and.operands.append(dest)
        and_expr.bool_and.operands.append(add)
        dest.CopyFrom(and_expr)
