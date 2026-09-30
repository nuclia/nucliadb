from .models import MarkLogicDatabaseLocator
from .settings import META_MARKLOGIC_SERVER_ID

SYSTEM_DATABASE_NAME = "data-platform-content"
SYSTEM_SCHEMA_DATABASE_NAME = "data-platform-schemas"

SYSTEM_DATABASE = MarkLogicDatabaseLocator(META_MARKLOGIC_SERVER_ID, SYSTEM_DATABASE_NAME)
SYSTEM_SCHEMA_DATABASE = MarkLogicDatabaseLocator(META_MARKLOGIC_SERVER_ID, SYSTEM_SCHEMA_DATABASE_NAME)
