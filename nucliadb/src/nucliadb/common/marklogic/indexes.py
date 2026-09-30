import logging
from dataclasses import dataclass, field

from .exceptions import MarkLogicError
from .models import MarkLogicDatabaseLocator
from .utils import get_httpx_admin_client

logger = logging.getLogger(__name__)

STRING_COLLATION = "http://marklogic.com/collation/codepoint"


@dataclass
class RangeElementIndex:
    scalar_type: str
    localname: str
    namespace_uri: str = ""
    collation: str = ""
    range_value_positions: bool = False
    invalid_values: str = "ignore"


@dataclass
class RangePathIndex:
    scalar_type: str
    path_expression: str
    collation: str = ""
    range_value_positions: bool = False
    invalid_values: str = "ignore"


def _index_to_dict(index: RangeElementIndex) -> dict:
    return {
        "scalar-type": index.scalar_type,
        "namespace-uri": index.namespace_uri,
        "localname": index.localname,
        "collation": index.collation,
        "range-value-positions": index.range_value_positions,
        "invalid-values": index.invalid_values,
    }


def _index_from_dict(d: dict) -> RangeElementIndex:
    return RangeElementIndex(
        scalar_type=d.get("scalar-type", ""),
        localname=d.get("localname", ""),
        namespace_uri=d.get("namespace-uri", ""),
        collation=d.get("collation", ""),
        range_value_positions=d.get("range-value-positions", False),
        invalid_values=d.get("invalid-values") or "ignore",
    )


async def get_range_element_indexes(db: MarkLogicDatabaseLocator) -> list[RangeElementIndex]:
    logger.info("Getting range element indexes", extra={"database": db.database})
    client = get_httpx_admin_client(db.server_id)
    resp = await client.get(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Accept": "application/json"},
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to get database properties: {resp.status_code} {resp.text}")
    data = resp.json()
    return [_index_from_dict(d) for d in data.get("range-element-index", [])]


async def add_range_element_indexes(
    db: MarkLogicDatabaseLocator, new_indexes: list[RangeElementIndex]
) -> None:
    existing = await get_range_element_indexes(db)

    existing_keys = {(idx.localname, idx.namespace_uri) for idx in existing}
    merged = list(existing)
    added = []
    for idx in new_indexes:
        key = (idx.localname, idx.namespace_uri)
        if key not in existing_keys:
            merged.append(idx)
            existing_keys.add(key)
            added.append(idx.localname)

    if not added:
        logger.info(
            "All proposed range indexes already exist, skipping", extra={"database": db.database}
        )
        return

    logger.info(
        "Adding range element indexes via Manage API",
        extra={"database": db.database, "adding": added, "total_after": len(merged)},
    )
    client = get_httpx_admin_client(db.server_id)
    resp = await client.put(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"range-element-index": [_index_to_dict(idx) for idx in merged]},
        timeout=60.0,
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to update database properties: {resp.status_code} {resp.text}")


def _path_index_to_dict(index: RangePathIndex) -> dict:
    return {
        "scalar-type": index.scalar_type,
        "path-expression": index.path_expression,
        "collation": index.collation,
        "range-value-positions": index.range_value_positions,
        "invalid-values": index.invalid_values,
    }


def _path_index_from_dict(d: dict) -> RangePathIndex:
    return RangePathIndex(
        scalar_type=d.get("scalar-type", ""),
        path_expression=d.get("path-expression", ""),
        collation=d.get("collation", ""),
        range_value_positions=d.get("range-value-positions", False),
        invalid_values=d.get("invalid-values") or "ignore",
    )


async def get_range_path_indexes(db: MarkLogicDatabaseLocator) -> list[RangePathIndex]:
    logger.info("Getting range path indexes", extra={"database": db.database})
    client = get_httpx_admin_client(db.server_id)
    resp = await client.get(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Accept": "application/json"},
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to get database properties: {resp.status_code} {resp.text}")
    data = resp.json()
    return [_path_index_from_dict(d) for d in data.get("range-path-index", [])]


async def add_range_path_indexes(
    db: MarkLogicDatabaseLocator, new_indexes: list[RangePathIndex]
) -> None:
    existing = await get_range_path_indexes(db)

    existing_keys = {(idx.path_expression, idx.scalar_type, idx.collation) for idx in existing}
    merged = list(existing)
    added = []
    for idx in new_indexes:
        key = (idx.path_expression, idx.scalar_type, idx.collation)
        if key not in existing_keys:
            merged.append(idx)
            existing_keys.add(key)
            added.append(key)

    if not added:
        logger.info(
            "All proposed path range indexes already exist, skipping", extra={"database": db.database}
        )
        return

    logger.info(
        "Adding range path indexes via Manage API",
        extra={"database": db.database, "adding": added, "total_after": len(merged)},
    )
    client = get_httpx_admin_client(db.server_id)
    resp = await client.put(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"range-path-index": [_path_index_to_dict(idx) for idx in merged]},
        timeout=60.0,
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to update database properties: {resp.status_code} {resp.text}")


@dataclass
class GeoElementIndex:
    """A MarkLogic geospatial element index for a JSON property containing a lat/lon point string."""

    localname: str
    namespace_uri: str = ""
    coordinate_system: str = "wgs84"
    point_format: str = "point"
    range_value_positions: bool = False
    invalid_values: str = "reject"


@dataclass
class GeoElementChildIndex:
    """A MarkLogic geospatial-element-child-index for a nested JSON property (parent/child path)."""

    parent_localname: str
    localname: str
    parent_namespace_uri: str = ""
    namespace_uri: str = ""
    coordinate_system: str = "wgs84"
    point_format: str = "point"
    range_value_positions: bool = False
    invalid_values: str = "reject"


def _geo_index_to_dict(index: GeoElementIndex) -> dict:
    return {
        "namespace-uri": index.namespace_uri,
        "localname": index.localname,
        "coordinate-system": index.coordinate_system,
        "point-format": index.point_format,
        "range-value-positions": index.range_value_positions,
        "invalid-values": index.invalid_values,
    }


def _geo_index_from_dict(d: dict) -> GeoElementIndex:
    return GeoElementIndex(
        localname=d.get("localname", ""),
        namespace_uri=d.get("namespace-uri", ""),
        coordinate_system=d.get("coordinate-system", "wgs84"),
        point_format=d.get("point-format", "point"),
        range_value_positions=d.get("range-value-positions", False),
        invalid_values=d.get("invalid-values") or "reject",
    )


async def get_geo_element_indexes(db: MarkLogicDatabaseLocator) -> list[GeoElementIndex]:
    logger.info("Getting geospatial element indexes", extra={"database": db.database})
    client = get_httpx_admin_client(db.server_id)
    resp = await client.get(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Accept": "application/json"},
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to get database properties: {resp.status_code} {resp.text}")
    data = resp.json()
    return [_geo_index_from_dict(d) for d in data.get("geospatial-element-index", [])]


async def add_geo_element_indexes(
    db: MarkLogicDatabaseLocator, new_indexes: list[GeoElementIndex]
) -> None:
    """Add geospatial element indexes to the database, skipping any that already exist."""
    existing = await get_geo_element_indexes(db)

    existing_keys = {(idx.localname, idx.namespace_uri) for idx in existing}
    merged = list(existing)
    added = []
    for idx in new_indexes:
        key = (idx.localname, idx.namespace_uri)
        if key not in existing_keys:
            merged.append(idx)
            existing_keys.add(key)
            added.append(idx.localname)

    if not added:
        logger.info("All proposed geo indexes already exist, skipping", extra={"database": db.database})
        return

    logger.info(
        "Adding geospatial element indexes via Manage API",
        extra={"database": db.database, "adding": added, "total_after": len(merged)},
    )
    client = get_httpx_admin_client(db.server_id)
    resp = await client.put(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"geospatial-element-index": [_geo_index_to_dict(idx) for idx in merged]},
        timeout=60.0,
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to update database properties: {resp.status_code} {resp.text}")


def _geo_child_index_to_dict(index: GeoElementChildIndex) -> dict:
    return {
        "parent-namespace-uri": index.parent_namespace_uri,
        "parent-localname": index.parent_localname,
        "namespace-uri": index.namespace_uri,
        "localname": index.localname,
        "coordinate-system": index.coordinate_system,
        "point-format": index.point_format,
        "range-value-positions": index.range_value_positions,
        "invalid-values": index.invalid_values,
    }


def _geo_child_index_from_dict(d: dict) -> GeoElementChildIndex:
    return GeoElementChildIndex(
        parent_localname=d.get("parent-localname", ""),
        localname=d.get("localname", ""),
        parent_namespace_uri=d.get("parent-namespace-uri", ""),
        namespace_uri=d.get("namespace-uri", ""),
        coordinate_system=d.get("coordinate-system", "wgs84"),
        point_format=d.get("point-format", "point"),
        range_value_positions=d.get("range-value-positions", False),
        invalid_values=d.get("invalid-values") or "reject",
    )


async def get_geo_element_child_indexes(db: MarkLogicDatabaseLocator) -> list[GeoElementChildIndex]:
    logger.info("Getting geospatial element child indexes", extra={"database": db.database})
    client = get_httpx_admin_client(db.server_id)
    resp = await client.get(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Accept": "application/json"},
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to get database properties: {resp.status_code} {resp.text}")
    data = resp.json()
    return [_geo_child_index_from_dict(d) for d in data.get("geospatial-element-child-index", [])]


async def add_geo_element_child_indexes(
    db: MarkLogicDatabaseLocator, new_indexes: list[GeoElementChildIndex]
) -> None:
    """Add geospatial element child indexes to the database, skipping any that already exist."""
    existing = await get_geo_element_child_indexes(db)

    existing_keys = {(idx.parent_localname, idx.localname, idx.namespace_uri) for idx in existing}
    merged = list(existing)
    added = []
    for idx in new_indexes:
        key = (idx.parent_localname, idx.localname, idx.namespace_uri)
        if key not in existing_keys:
            merged.append(idx)
            existing_keys.add(key)
            added.append(f"{idx.parent_localname}/{idx.localname}")

    if not added:
        logger.info(
            "All proposed geo child indexes already exist, skipping", extra={"database": db.database}
        )
        return

    logger.info(
        "Adding geospatial element child indexes via Manage API",
        extra={"database": db.database, "adding": added, "total_after": len(merged)},
    )
    client = get_httpx_admin_client(db.server_id)
    resp = await client.put(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"geospatial-element-child-index": [_geo_child_index_to_dict(idx) for idx in merged]},
        timeout=60.0,
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to update database properties: {resp.status_code} {resp.text}")


@dataclass
class GeoElementPairIndex:
    """A MarkLogic geospatial-element-pair-index for sibling lat/lon JSON properties."""

    latitude_localname: str
    longitude_localname: str
    parent_localname: str = ""
    parent_namespace_uri: str = ""
    latitude_namespace_uri: str = ""
    longitude_namespace_uri: str = ""
    coordinate_system: str = "wgs84"
    range_value_positions: bool = False
    invalid_values: str = "ignore"


def _geo_pair_index_to_dict(index: GeoElementPairIndex) -> dict:
    return {
        "parent-namespace-uri": index.parent_namespace_uri,
        "parent-localname": index.parent_localname,
        "latitude-namespace-uri": index.latitude_namespace_uri,
        "latitude-localname": index.latitude_localname,
        "longitude-namespace-uri": index.longitude_namespace_uri,
        "longitude-localname": index.longitude_localname,
        "coordinate-system": index.coordinate_system,
        "range-value-positions": index.range_value_positions,
        "invalid-values": index.invalid_values,
    }


def _geo_pair_index_from_dict(d: dict) -> GeoElementPairIndex:
    return GeoElementPairIndex(
        latitude_localname=d.get("latitude-localname", ""),
        longitude_localname=d.get("longitude-localname", ""),
        parent_localname=d.get("parent-localname", ""),
        parent_namespace_uri=d.get("parent-namespace-uri", ""),
        latitude_namespace_uri=d.get("latitude-namespace-uri", ""),
        longitude_namespace_uri=d.get("longitude-namespace-uri", ""),
        coordinate_system=d.get("coordinate-system", "wgs84"),
        range_value_positions=d.get("range-value-positions", False),
        invalid_values=d.get("invalid-values") or "ignore",
    )


async def get_geo_element_pair_indexes(db: MarkLogicDatabaseLocator) -> list[GeoElementPairIndex]:
    logger.info("Getting geospatial element pair indexes", extra={"database": db.database})
    client = get_httpx_admin_client(db.server_id)
    resp = await client.get(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Accept": "application/json"},
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to get database properties: {resp.status_code} {resp.text}")
    data = resp.json()
    return [_geo_pair_index_from_dict(d) for d in data.get("geospatial-element-pair-index", [])]


async def add_geo_element_pair_indexes(
    db: MarkLogicDatabaseLocator, new_indexes: list[GeoElementPairIndex]
) -> None:
    """Add geospatial element pair indexes to the database, skipping any that already exist."""
    existing = await get_geo_element_pair_indexes(db)

    existing_keys = {
        (idx.parent_localname, idx.latitude_localname, idx.longitude_localname, idx.parent_namespace_uri)
        for idx in existing
    }
    merged = list(existing)
    added = []
    for idx in new_indexes:
        key = (
            idx.parent_localname,
            idx.latitude_localname,
            idx.longitude_localname,
            idx.parent_namespace_uri,
        )
        if key not in existing_keys:
            merged.append(idx)
            existing_keys.add(key)
            added.append(f"{idx.parent_localname}/{idx.latitude_localname},{idx.longitude_localname}")

    if not added:
        logger.info(
            "All proposed geo pair indexes already exist, skipping", extra={"database": db.database}
        )
        return

    logger.info(
        "Adding geospatial element pair indexes via Manage API",
        extra={"database": db.database, "adding": added, "total_after": len(merged)},
    )
    client = get_httpx_admin_client(db.server_id)
    resp = await client.put(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"geospatial-element-pair-index": [_geo_pair_index_to_dict(idx) for idx in merged]},
        timeout=60.0,
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to update database properties: {resp.status_code} {resp.text}")


@dataclass
class GeoRegionPathIndex:
    """A MarkLogic geospatial-region-path-index for WKT (or other region) strings."""

    path_expression: str
    coordinate_system: str = "wgs84"
    geohash_precision: int = 2
    invalid_values: str = "reject"


def _geo_region_path_index_to_dict(index: GeoRegionPathIndex) -> dict:
    return {
        "path-expression": index.path_expression,
        "coordinate-system": index.coordinate_system,
        "geohash-precision": index.geohash_precision,
        "invalid-values": index.invalid_values,
    }


def _geo_region_path_index_from_dict(d: dict) -> GeoRegionPathIndex:
    return GeoRegionPathIndex(
        path_expression=d.get("path-expression", ""),
        coordinate_system=d.get("coordinate-system", "wgs84"),
        geohash_precision=int(d.get("geohash-precision") or 2),
        invalid_values=d.get("invalid-values") or "reject",
    )


async def get_geo_region_path_indexes(db: MarkLogicDatabaseLocator) -> list[GeoRegionPathIndex]:
    logger.info("Getting geospatial region path indexes", extra={"database": db.database})
    client = get_httpx_admin_client(db.server_id)
    resp = await client.get(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Accept": "application/json"},
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to get database properties: {resp.status_code} {resp.text}")
    data = resp.json()
    return [_geo_region_path_index_from_dict(d) for d in data.get("geospatial-region-path-index", [])]


async def add_geo_region_path_indexes(
    db: MarkLogicDatabaseLocator, new_indexes: list[GeoRegionPathIndex]
) -> None:
    """Add geospatial region path indexes to the database, skipping any that already exist."""
    existing = await get_geo_region_path_indexes(db)

    existing_keys = {(idx.path_expression, idx.coordinate_system) for idx in existing}
    merged = list(existing)
    added = []
    for idx in new_indexes:
        key = (idx.path_expression, idx.coordinate_system)
        if key not in existing_keys:
            merged.append(idx)
            existing_keys.add(key)
            added.append(idx.path_expression)

    if not added:
        logger.info(
            "All proposed geo region path indexes already exist, skipping",
            extra={"database": db.database},
        )
        return

    logger.info(
        "Adding geospatial region path indexes via Manage API",
        extra={"database": db.database, "adding": added, "total_after": len(merged)},
    )
    client = get_httpx_admin_client(db.server_id)
    resp = await client.put(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"geospatial-region-path-index": [_geo_region_path_index_to_dict(idx) for idx in merged]},
        timeout=60.0,
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to update database properties: {resp.status_code} {resp.text}")


@dataclass
class PathNamespace:
    """A ``path-namespace`` prefix/URI binding used by field paths and path indexes."""

    prefix: str
    namespace_uri: str


def path_namespace_to_dict(namespace: PathNamespace) -> dict:
    return {"prefix": namespace.prefix, "namespace-uri": namespace.namespace_uri}


def path_namespace_from_dict(d: dict) -> PathNamespace:
    return PathNamespace(prefix=d.get("prefix", ""), namespace_uri=d.get("namespace-uri", ""))


@dataclass
class FieldPath:
    """One of a ``field``'s ``field-path`` entries (a path contributing to that field's value)."""

    path: str
    weight: float = 1.0


@dataclass
class Field:
    """A MarkLogic ``field`` definition: either a metadata-only field (e.g. incremental-write
    hashes) or a union of one or more document paths (e.g. the same logical property at both
    its JSON and namespaced-XML locations).
    """

    field_name: str
    field_paths: list[FieldPath] = field(default_factory=list)
    metadata: str | None = None
    field_value_searches: bool = False
    fast_phrase_searches: bool = False
    fast_case_sensitive_searches: bool = False
    fast_diacritic_sensitive_searches: bool = False
    trailing_wildcard_searches: bool = False
    trailing_wildcard_word_positions: bool = False


def field_to_dict(f: Field) -> dict:
    d: dict = {"field-name": f.field_name}
    if f.metadata is not None:
        d["metadata"] = f.metadata
    if f.field_paths:
        d["field-path"] = [{"path": fp.path, "weight": fp.weight} for fp in f.field_paths]
        d["field-value-searches"] = f.field_value_searches
        d["fast-phrase-searches"] = f.fast_phrase_searches
        d["fast-case-sensitive-searches"] = f.fast_case_sensitive_searches
        d["fast-diacritic-sensitive-searches"] = f.fast_diacritic_sensitive_searches
        d["trailing-wildcard-searches"] = f.trailing_wildcard_searches
        d["trailing-wildcard-word-positions"] = f.trailing_wildcard_word_positions
    return d


def field_from_dict(d: dict) -> Field:
    return Field(
        field_name=d.get("field-name", ""),
        field_paths=[
            FieldPath(path=fp.get("path", ""), weight=fp.get("weight", 1.0))
            for fp in d.get("field-path", [])
        ],
        metadata=d.get("metadata"),
        field_value_searches=d.get("field-value-searches", False),
        fast_phrase_searches=d.get("fast-phrase-searches", False),
        fast_case_sensitive_searches=d.get("fast-case-sensitive-searches", False),
        fast_diacritic_sensitive_searches=d.get("fast-diacritic-sensitive-searches", False),
        trailing_wildcard_searches=d.get("trailing-wildcard-searches", False),
        trailing_wildcard_word_positions=d.get("trailing-wildcard-word-positions", False),
    )


@dataclass
class RangeFieldIndex:
    scalar_type: str
    field_name: str
    collation: str = ""
    range_value_positions: bool = False
    invalid_values: str = "ignore"


def range_field_index_to_dict(index: RangeFieldIndex) -> dict:
    return {
        "scalar-type": index.scalar_type,
        "field-name": index.field_name,
        "collation": index.collation,
        "range-value-positions": index.range_value_positions,
        "invalid-values": index.invalid_values,
    }


def range_field_index_from_dict(d: dict) -> RangeFieldIndex:
    return RangeFieldIndex(
        scalar_type=d.get("scalar-type", ""),
        field_name=d.get("field-name", ""),
        collation=d.get("collation", ""),
        range_value_positions=d.get("range-value-positions", False),
        invalid_values=d.get("invalid-values") or "ignore",
    )


@dataclass
class DatabaseIndexUpdate:
    """A batch of additive/subtractive changes to apply to a database's index/field/namespace
    properties in a single :func:`update_database_index_properties` call.

    ``remove_*`` entries are matched for removal by the same natural dedup key each property
    type already uses to detect "already exists" (e.g. localname+namespace-uri for element
    indexes, path+scalar-type+collation for path indexes) -- only exact matches are removed;
    everything else on the database is left untouched.
    """

    add_range_element_indexes: list[RangeElementIndex] = field(default_factory=list)
    remove_range_element_indexes: list[RangeElementIndex] = field(default_factory=list)
    add_range_path_indexes: list[RangePathIndex] = field(default_factory=list)
    remove_range_path_indexes: list[RangePathIndex] = field(default_factory=list)
    add_fields: list[Field] = field(default_factory=list)
    add_range_field_indexes: list[RangeFieldIndex] = field(default_factory=list)
    add_path_namespaces: list[PathNamespace] = field(default_factory=list)


async def update_database_index_properties(
    db: MarkLogicDatabaseLocator, update: DatabaseIndexUpdate
) -> None:
    """Apply additive/subtractive index, field, and namespace changes to a database's
    properties in a single GET + single PUT, touching only the property lists that actually
    change.

    Add operations skip entries that already exist (by the same dedup key used elsewhere in
    this module for that property type); remove operations only drop entries that exactly
    match one of ``update``'s ``remove_*`` entries, leaving everything else (including any
    customer- or future-added index) untouched.

    Existing entries that are kept are preserved as the exact raw dicts returned by the GET
    (not round-tripped through our dataclasses), since MarkLogic's real property shapes --
    especially ``field`` entries, including the built-in default/root field, which always
    has an empty ``field-name`` -- can carry attributes our dataclasses don't model. Fully
    reparsing and reserializing them would silently drop those attributes and, at least for
    ``field``, cause the Manage API to reject the PUT (``ADMIN-INVALIDFIELDNAME``).
    """
    client = get_httpx_admin_client(db.server_id)
    resp = await client.get(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Accept": "application/json"},
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to get database properties: {resp.status_code} {resp.text}")
    data = resp.json()

    payload: dict = {}

    if update.add_range_element_indexes or update.remove_range_element_indexes:
        existing_elem = data.get("range-element-index", [])
        remove_elem_keys = {
            (idx.localname, idx.namespace_uri) for idx in update.remove_range_element_indexes
        }
        merged_elem = [
            d
            for d in existing_elem
            if (d.get("localname", ""), d.get("namespace-uri", "")) not in remove_elem_keys
        ]
        seen_elem = {(d.get("localname", ""), d.get("namespace-uri", "")) for d in merged_elem}
        for idx in update.add_range_element_indexes:
            key = (idx.localname, idx.namespace_uri)
            if key not in seen_elem:
                merged_elem.append(_index_to_dict(idx))
                seen_elem.add(key)
        if merged_elem != existing_elem:
            payload["range-element-index"] = merged_elem

    if update.add_range_path_indexes or update.remove_range_path_indexes:
        existing_path = data.get("range-path-index", [])
        remove_path_keys = {
            (idx.path_expression, idx.scalar_type, idx.collation)
            for idx in update.remove_range_path_indexes
        }
        merged_path = [
            d
            for d in existing_path
            if (d.get("path-expression", ""), d.get("scalar-type", ""), d.get("collation", ""))
            not in remove_path_keys
        ]
        seen_path = {
            (d.get("path-expression", ""), d.get("scalar-type", ""), d.get("collation", ""))
            for d in merged_path
        }
        for path_idx in update.add_range_path_indexes:
            path_key = (path_idx.path_expression, path_idx.scalar_type, path_idx.collation)
            if path_key not in seen_path:
                merged_path.append(_path_index_to_dict(path_idx))
                seen_path.add(path_key)
        if merged_path != existing_path:
            payload["range-path-index"] = merged_path

    if update.add_fields:
        existing_fields = data.get("field", [])
        merged_fields = list(existing_fields)
        seen_fields = {d.get("field-name", "") for d in merged_fields}
        for f in update.add_fields:
            if f.field_name not in seen_fields:
                merged_fields.append(field_to_dict(f))
                seen_fields.add(f.field_name)
        if merged_fields != existing_fields:
            payload["field"] = merged_fields

    if update.add_range_field_indexes:
        existing_range_field = data.get("range-field-index", [])
        merged_range_field = list(existing_range_field)
        seen_range_field = {d.get("field-name", "") for d in merged_range_field}
        for field_idx in update.add_range_field_indexes:
            if field_idx.field_name not in seen_range_field:
                merged_range_field.append(range_field_index_to_dict(field_idx))
                seen_range_field.add(field_idx.field_name)
        if merged_range_field != existing_range_field:
            payload["range-field-index"] = merged_range_field

    if update.add_path_namespaces:
        existing_namespaces = data.get("path-namespace", [])
        merged_namespaces = list(existing_namespaces)
        seen_namespaces = {d.get("prefix", "") for d in merged_namespaces}
        for ns in update.add_path_namespaces:
            if ns.prefix not in seen_namespaces:
                merged_namespaces.append(path_namespace_to_dict(ns))
                seen_namespaces.add(ns.prefix)
        if merged_namespaces != existing_namespaces:
            payload["path-namespace"] = merged_namespaces

    if not payload:
        logger.info("No database property changes needed, skipping", extra={"database": db.database})
        return

    logger.info(
        "Updating database index/field/namespace properties via Manage API",
        extra={"database": db.database, "changed_properties": list(payload.keys())},
    )
    resp = await client.put(
        f"/manage/v2/databases/{db.database}/properties",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json=payload,
        timeout=60.0,
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to update database properties: {resp.status_code} {resp.text}")
