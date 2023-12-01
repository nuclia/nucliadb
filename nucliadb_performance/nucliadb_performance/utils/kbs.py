import os
from enum import Enum
from functools import cache


class TestKBSlugs(Enum):
    TINY = "tiny"
    SMALL = "small"
    MEDIUM = "medium"
    TINY_EXPERIMENTAL = "experimental-tiny"
    SMALL_EXPERIMENTAL = "experimental-small"
    MEDIUM_EXPERIMENTAL = "experimental-medium"


DEFAULT_KBS = [slug.value for slug in TestKBSlugs]


@cache
def parse_input_kb_slug() -> str:
    slug = os.environ.get("KB_SLUG", "")
    if slug == "":
        slug = input(
            f"Enter the slug to test with or choose among the default ones {DEFAULT_KBS}: "
        )
    return slug
