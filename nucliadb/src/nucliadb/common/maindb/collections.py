from enum import Enum


class MarkLogicCollections(str, Enum):
    KB_REGISTRY_ITEM = "kb_registry_item"
    KNOWLEDGEBOXES = "knowledgeboxes"
    RESOURCES = "resources"
    FIELDS = "fields"
    FIELDS_PROCESSED = "fields_processed"
    CONVERSATIONS = "conversations"
    CHUNKS = "chunks"
    EMBEDDINGS = "embeddings"
    MAINDB = "maindb"
