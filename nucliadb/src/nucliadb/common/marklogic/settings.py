import os
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

META_MARKLOGIC_SERVER_ID = "META"


class MarkLogicServerSettings(BaseModel):
    uri: str
    username: str | None = None
    password: SecretStr | None = None
    admin_port: int = 8002
    init_port: int = 8001
    system_port: int = 8003


class MarkLogicDataServerSettings(MarkLogicServerSettings):
    id: str


class ServerNotFoundError(Exception):
    pass


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=os.getenv("ENV_FILE", ".env"),
        env_nested_delimiter="__",
    )

    # Dummy defaults so importing the package does not require MarkLogic env settings
    marklogic_meta_server: MarkLogicServerSettings = Field(
        default_factory=lambda: MarkLogicServerSettings(
            uri="http://localhost", username="admin", password=SecretStr("admin")
        )
    )
    marklogic_data_servers: list[MarkLogicDataServerSettings] = Field(
        default_factory=lambda: [
            MarkLogicDataServerSettings(
                id="default", uri="http://localhost", username="admin", password=SecretStr("admin")
            )
        ],
        min_length=1,
    )

    @model_validator(mode="after")
    def _apply_credential_overrides(self) -> "EnvSettings":
        """
        Apply per-field credential overrides from flat env vars.

        pydantic-settings' env_nested_delimiter does not support list index
        notation (e.g. MARKLOGIC_DATA_SERVERS__0__PASSWORD) for list[BaseModel]
        fields.  Instead we read explicit override vars and patch the already-
        parsed models:

          MARKLOGIC_META_SERVER__USERNAME   - overrides meta server username
          MARKLOGIC_META_SERVER__PASSWORD   - overrides meta server password
          MARKLOGIC_DATA_SERVER_<n>_USERNAME - overrides data server n username
          MARKLOGIC_DATA_SERVER_<n>_PASSWORD - overrides data server n password
        """
        prefix = "MARKLOGIC_META_SERVER__"
        username_override = os.environ.get(f"{prefix}USERNAME")
        password_override = os.environ.get(f"{prefix}PASSWORD")
        if username_override is not None or password_override is not None:
            overrides: dict[str, str | SecretStr] = {}
            if username_override is not None:
                overrides["username"] = username_override
            if password_override is not None:
                overrides["password"] = SecretStr(password_override)
            self.marklogic_meta_server = self.marklogic_meta_server.model_copy(update=overrides)

        for i, server in enumerate(self.marklogic_data_servers):
            prefix = f"MARKLOGIC_DATA_SERVER_{i}_"
            username_override = os.environ.get(f"{prefix}USERNAME")
            password_override = os.environ.get(f"{prefix}PASSWORD")
            if username_override is not None or password_override is not None:
                overrides = {}
                if username_override is not None:
                    overrides["username"] = username_override
                if password_override is not None:
                    overrides["password"] = SecretStr(password_override)
                self.marklogic_data_servers[i] = server.model_copy(update=overrides)

        # Enforce that credentials are present after overrides have been applied.
        errors: list[str] = []
        if not self.marklogic_meta_server.username:
            errors.append("marklogic_meta_server.username is required")
        if not self.marklogic_meta_server.password:
            errors.append("marklogic_meta_server.password is required")
        for i, server in enumerate(self.marklogic_data_servers):
            if not server.username:
                errors.append(f"marklogic_data_servers.{i}.username is required")
            if not server.password:
                errors.append(f"marklogic_data_servers.{i}.password is required")
        if errors:
            raise ValueError("; ".join(errors))

        return self

    def get_marklogic_server(self, server_id: str) -> MarkLogicServerSettings:
        if server_id == META_MARKLOGIC_SERVER_ID:
            return self.marklogic_meta_server
        for server in self.marklogic_data_servers:
            if server.id == server_id:
                return server
        raise ServerNotFoundError(f"Unknown MarkLogic server: {server_id}")


if TYPE_CHECKING:
    env_settings = EnvSettings(
        marklogic_meta_server=MarkLogicServerSettings(uri="", username="", password=SecretStr("")),
        marklogic_data_servers=[
            MarkLogicDataServerSettings(id="default", uri="", username="", password=SecretStr(""))
        ],
    )
else:
    env_settings = EnvSettings()
