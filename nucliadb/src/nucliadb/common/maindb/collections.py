class MarkLogicCollections:
    KNOWLEDGEBOXES = "knowledgeboxes"
    RESOURCES = "resources"
    MAINDB = "nucliadb-maindb"

    @classmethod
    def all(cls) -> tuple[str, ...]:
        return (cls.KNOWLEDGEBOXES, cls.RESOURCES, cls.MAINDB)
