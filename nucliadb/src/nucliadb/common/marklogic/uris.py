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
class MarkLogicURIs:
    """Canonical document URI builders and parsers for the MarkLogic schema.

    Examples:
        KB registry:       ``/kb/kbid-123/registry.json``
        KB data:           ``/kb_data.json``
        Resource:          ``/resources/resource-123.json``
        Field:             ``/resources/resource-123/fields/t/body/data.json``
        Conversation page: ``/resources/resource-123/fields/c/chat/pages/1.json``
        Processed field:   ``/resources/resource-123/fields/t/body/processed.json``
        Chunk:             ``/resources/resource-123/fields/t/body/chunks/chunk-1/data.json``
        Embedding:         ``/resources/resource-123/fields/t/body/embeddings/model-1/chunk-1/data.json``
    """

    @classmethod
    def maindb_key(cls, key: str) -> str:
        return f"maindb:{key}"

    @classmethod
    def maindb_key_from_uri(cls, uri: str) -> str:
        return uri.removeprefix("maindb:")

    @classmethod
    def kb_registry(cls, kbid: str) -> str:
        return f"/kb/{kbid}/registry.json"

    @classmethod
    def kbid_from_registry(cls, uri: str) -> str:
        parts = uri.strip("/").split("/")
        if len(parts) != 3 or parts[0] != "kb" or parts[2] != "registry.json":
            raise ValueError(f"Invalid registry URI: {uri}")
        return parts[1]

    @classmethod
    def kb_data(cls) -> str:
        return "/kb_data.json"

    @classmethod
    def resource_directory(cls, rid: str) -> str:
        return f"/resources/{rid}/"

    @classmethod
    def resource(cls, rid: str) -> str:
        return f"/resources/{rid}.json"

    @classmethod
    def resource_id_from_uri(cls, uri: object) -> str:
        return str(uri).rsplit("/", 1)[-1].removesuffix(".json")

    @classmethod
    def fields_directory(cls, rid: str) -> str:
        return f"{cls.resource_directory(rid)}fields/"

    @classmethod
    def field_directory(cls, rid: str, field_type: str, field_id: str) -> str:
        return f"{cls.fields_directory(rid)}{field_type}/{field_id}/"

    @classmethod
    def field(cls, rid: str, field_type: str, field_id: str) -> str:
        return f"{cls.field_directory(rid, field_type, field_id)}data.json"

    @classmethod
    def conversation_directory(cls, rid: str, field_id: str) -> str:
        return f"{cls.field_directory(rid, 'c', field_id)}pages/"

    @classmethod
    def conversation_page(cls, rid: str, field_id: str, page: int) -> str:
        return f"{cls.conversation_directory(rid, field_id)}{page}.json"

    @classmethod
    def field_processed(cls, rid: str, field_id: str, field_type: str) -> str:
        return f"{cls.field_directory(rid, field_type, field_id)}processed.json"

    @classmethod
    def chunks_directory(cls, rid: str, field_type: str, field_id: str) -> str:
        return f"{cls.field_directory(rid, field_type, field_id)}chunks/"

    @classmethod
    def _field_id_suffix(
        cls,
        rid: str,
        field_type: str,
        field_id: str,
        identifier: str,
        *,
        has_index: bool = False,
    ) -> str:
        """
        >>> MarkLogicURIs._field_id_suffix("rid", "t", "body", "rid/t/body/12-34")
        '12-34'
        >>> MarkLogicURIs._field_id_suffix("rid", "c", "chat", "rid/c/chat/message-1/12-34")
        'message-1/12-34'
        >>> MarkLogicURIs._field_id_suffix("rid", "t", "body", "rid/t/body/2/12-34", has_index=True)
        '2/12-34'
        >>> MarkLogicURIs._field_id_suffix("rid", "c", "chat", "rid/c/chat/message-1/2/12-34", has_index=True)
        'message-1/2/12-34'
        """
        field_prefix = f"{rid}/{field_type}/{field_id}/"
        if not identifier.startswith(field_prefix):
            raise ValueError(f"ID {identifier!r} does not belong to field {field_prefix!r}")

        suffix_parts = identifier.removeprefix(field_prefix).split("/")
        valid_part_counts = (2, 3) if has_index else (1, 2)
        if len(suffix_parts) not in valid_part_counts or any(not part for part in suffix_parts):
            raise ValueError(f"Invalid ID: {identifier!r}")

        chunk_range = suffix_parts[-1]
        try:
            start, end = chunk_range.split("-", 1)
            int(start)
            int(end)
        except ValueError as exc:
            raise ValueError(f"Invalid range in ID: {identifier!r}") from exc

        if has_index:
            index_position = -2
            if not suffix_parts[index_position].isdigit():
                raise ValueError(f"Invalid index in ID: {identifier!r}")
        return "/".join(suffix_parts)

    @classmethod
    def chunk(cls, rid: str, field_type: str, field_id: str, chunk_id: str) -> str:
        suffix = cls._field_id_suffix(rid, field_type, field_id, chunk_id)
        return f"{cls.chunks_directory(rid, field_type, field_id)}{suffix}/data.json"

    @classmethod
    def embeddings_directory(
        cls, rid: str, field_type: str, field_id: str, model_id: str | None = None
    ) -> str:
        directory = f"{cls.field_directory(rid, field_type, field_id)}embeddings/"
        return directory if model_id is None else f"{directory}{model_id}/"

    @classmethod
    def embedding(
        cls, rid: str, field_type: str, field_id: str, model_id: str, embedding_id: str
    ) -> str:
        suffix = cls._field_id_suffix(rid, field_type, field_id, embedding_id, has_index=True)
        return f"{cls.embeddings_directory(rid, field_type, field_id, model_id)}{suffix}/data.json"
