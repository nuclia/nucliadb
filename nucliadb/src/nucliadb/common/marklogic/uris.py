class MarkLogicURIs:
    """Canonical document URI builders and parsers for the MarkLogic schema.

    Examples:
        KB registry:       ``/kb/kbid-123/registry.json``
        KB data:           ``/kb_data.json``
        Resource:          ``/resources/resource-123.json``
        Field:             ``/resources/resource-123/fields/t/body/data.json``
        Conversation page: ``/resources/resource-123/fields/c/chat/pages/1.json``
        Processed field:   ``/resources/resource-123/fields/t/body/processed.json``
        Chunk:             ``/resources/resource-123/fields/c/body/chunks/chunk-1/data.json``
        Embedding:         ``/resources/resource-123/fields/e/body/embeddings/model-1/data.json``
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
    def embedding(cls, rid: str, field_id: str, embedding_id: str) -> str:
        return f"{cls.field_directory(rid, 'e', field_id)}embeddings/{embedding_id}/data.json"

    @classmethod
    def chunk(cls, rid: str, field_id: str, chunk_id: str) -> str:
        return f"{cls.field_directory(rid, 'c', field_id)}chunks/{chunk_id}/data.json"
