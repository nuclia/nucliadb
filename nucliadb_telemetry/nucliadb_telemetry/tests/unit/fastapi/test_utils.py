from nucliadb_telemetry.fastapi import utils
from fastapi import FastAPI

app = FastAPI()


@app.get("/api/v1/kb/{kbid}")
def get_kb(kbid: str):
    return {"kbid": kbid}


def test_get_path_template():
    scope = {
        "app": app,
        "path": "/api/v1/kb/123",
        "method": "GET",
        "type": "http",
    }

    path_template = utils.get_path_template(scope)
    assert path_template.path == "/api/v1/kb/{kbid}"
    assert path_template.match is True
    assert path_template.scope["path_params"] == {"kbid": "123"}
