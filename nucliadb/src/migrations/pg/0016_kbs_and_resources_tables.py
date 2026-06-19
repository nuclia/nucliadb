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

from nucliadb.common.maindb.pg import PGTransaction


async def migrate(txn: PGTransaction) -> None:
    """
    Create kbs, kb_resources and kb_fields tables.

    kbs
    ---
    One row per knowledge box.
      - kbid          Primary key
      - slug          Human-readable unique identifier (unique, nullable)
      - title         Display name
      - shards        Serialised protobuf — list of shard IDs
      - config        Serialised protobuf — KnowledgeBoxConfig

    kb_resources
    ------------
    One row per resource.  Foreign-keyed to kbs so that deleting a KB row
    removes all its resources automatically (ON DELETE CASCADE).
      - kbid          FK → kbs.kbid
      - rid           Resource UUID
      - slug          Optional human-readable slug
      - shard         Shard ID the resource belongs to
      - basic         Serialised resources_pb2.Basic
      - origin        Serialised resources_pb2.Origin
      - security      Serialised utils_pb2.Security
      - extra         Serialised resources_pb2.Extra
      - user_relations Serialised resources_pb2.Relations

    kb_fields
    ------
    One row per field in a resource.  Foreign-keyed to kb_resources so that
    deleting a resource (or its parent KB) cascades into kb_fields automatically.
      - kbid       FK → kb_resources.kbid
      - rid        FK → kb_resources.rid
      - field_type Single-char abbreviation: t=text, f=file, u=link,
                   c=conversation, a=generic, k=key_value
      - field_id   User-defined field name
      - status     Processing status matching FieldStatus.Status in writer.proto:
                   0=PENDING, 1=PROCESSED, 2=ERROR
      - value      Serialised protobuf bytes (excludes object-store data)
      - md5        Optional content hash; NULL when not provided; used for
                   duplicate detection within a knowledge box
    """
    async with txn.connection.cursor() as cur:
        # ------------------------------------------------------------------
        # kbs
        # ------------------------------------------------------------------
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS kbs (
                kbid   UUID NOT NULL,
                slug   TEXT,
                title  TEXT,
                shards BYTEA,
                config BYTEA,
                PRIMARY KEY (kbid)
            );
        """)

        await cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_kbs_slug
            ON kbs(slug)
            WHERE slug IS NOT NULL;
        """)

        # ------------------------------------------------------------------
        # kb_resources
        # ------------------------------------------------------------------
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS kb_resources (
                kbid           UUID NOT NULL,
                rid            UUID NOT NULL,
                slug           TEXT,
                shard          TEXT,
                basic          BYTEA,
                origin         BYTEA,
                security       BYTEA,
                extra          BYTEA,
                user_relations BYTEA,
                PRIMARY KEY (kbid, rid),
                FOREIGN KEY (kbid) REFERENCES kbs (kbid) ON DELETE CASCADE
            );
        """)

        # Fast slug-based lookups within a knowledge box
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_resources_slug
            ON kb_resources(kbid, slug)
            WHERE slug IS NOT NULL;
        """)

        # ------------------------------------------------------------------
        # kb_fields
        # ------------------------------------------------------------------
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS kb_fields (
                kbid       UUID     NOT NULL,
                rid        UUID     NOT NULL,
                field_type TEXT     NOT NULL,
                field_id   TEXT     NOT NULL,
                status     SMALLINT NOT NULL DEFAULT 0,
                value      BYTEA,
                md5        TEXT,
                PRIMARY KEY (kbid, rid, field_type, field_id),
                FOREIGN KEY (kbid, rid)
                    REFERENCES kb_resources (kbid, rid) ON DELETE CASCADE
            );
        """)

        # Fast lookup / deletion of all fields belonging to a resource
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_fields_resource
            ON kb_fields(kbid, rid);
        """)

        # Fast lookup of fields by type within a knowledge box
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_fields_kbid_type
            ON kb_fields(kbid, field_type);
        """)

        # Fast duplicate detection by MD5 within a knowledge box
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_kb_fields_md5
            ON kb_fields(kbid, md5)
            WHERE md5 IS NOT NULL;
        """)
