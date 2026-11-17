import pytest

from nucliadb.search.search import query_templates


def test_render_template_indents_multiline_fragments(monkeypatch):
    monkeypatch.setattr(query_templates, "load_template", lambda name: "(() => {\n  ${body}\n})()")

    rendered = query_templates.render_template("example.js", body="const value = 1;\nreturn value;")

    assert rendered == "(() => {\n  const value = 1;\n  return value;\n})()"


def test_render_template_preserves_dollars_in_inserted_javascript(monkeypatch):
    monkeypatch.setattr(query_templates, "load_template", lambda name: "${body}\nconst end = /$$/;")

    rendered = query_templates.render_template("example.js", body="const pattern = /value$/;")

    assert rendered == "const pattern = /value$/;\nconst end = /$/;"


def test_render_template_rejects_missing_values(monkeypatch):
    monkeypatch.setattr(query_templates, "load_template", lambda name: "${missing}")

    with pytest.raises(KeyError, match="missing"):
        query_templates.render_template("example.js")
