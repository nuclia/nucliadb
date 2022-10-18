from fastapi.applications import FastAPI

ROLE_METADATA_TEMPLATE = """
---
## Authorization roles
Authenticated user needs to fulfill one of this roles, otherwise the request will be rejected with a `403` response.
{scopes}
"""


def format_scopes(scope_list):
    return "\n".join(f"- `{scope}`" for scope in scope_list)


def extend_openapi(app: FastAPI):
    for route in app.routes:
        if hasattr(route.endpoint, "__required_scopes__"):
            scopes = route.endpoint.__required_scopes__
            route.description += ROLE_METADATA_TEMPLATE.format(
                scopes=format_scopes(scopes)
            )
