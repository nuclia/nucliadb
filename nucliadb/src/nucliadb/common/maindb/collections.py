from enum import Enum


class MarkLogicCollections(str, Enum):
    KB_REGISTRY_ITEM = "kb_registry_item"
    KNOWLEDGEBOXES = "knowledgeboxes"
    RESOURCES = "resources"
    FIELDS = "fields"
    CONVERSATIONS = "conversations"
    MAINDB = "maindb"
