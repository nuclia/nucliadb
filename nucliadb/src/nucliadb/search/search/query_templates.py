from functools import cache
from importlib.resources import files
from string import Template


@cache
def load_template(name: str) -> str:
    """Read an installed package asset once; raw helper files need no substitution."""
    return (
        files("nucliadb.search.search")
        .joinpath("templates", name)
        .read_text(encoding="utf-8")
        .rstrip("\n")
    )


def render_template(name: str, **values: object) -> str:
    """Substitute source fragments while keeping multiline JavaScript indented.

    This is source composition, not escaping or parameter binding. Callers supply
    trusted fragments, JSON literals or eval-variable references. Missing values
    raise rather than leaving unresolved placeholders in a server-side query.
    """
    source = load_template(name)

    def substitute(match) -> str:
        # Substitute only placeholders in the original file. Dollar signs inside
        # inserted JavaScript, such as regex anchors, must not be interpreted again.
        value = Template(match.group()).substitute(values)
        prefix = source[source.rfind("\n", 0, match.start()) + 1 : match.start()]
        indentation = prefix[: len(prefix) - len(prefix.lstrip(" \t"))]
        return value.replace("\n", "\n" + indentation)

    return Template.pattern.sub(substitute, source)
