import json
import re
from importlib.resources import files

LOADED_LANGUAGES = ("fr", "it", "es", "en", "ca", "de", "nl", "pt")
STOP_WORDS = frozenset(
    word
    for language in LOADED_LANGUAGES
    for word in json.loads(
        files("nucliadb.search.search")
        .joinpath("stop_words", f"{language}.json")
        .read_text(encoding="utf-8")
    )
)


def keyword_terms(text: str) -> list[str]:
    terms = re.findall(r'-?"[^"]*"|\S+', text)
    if not terms:
        return []
    # Keep the final token for prefix/fuzzy search, matching the Rust query parser.
    return [
        term for term in terms[:-1] if term.startswith(('"', "-")) or term.lower() not in STOP_WORDS
    ] + terms[-1:]
