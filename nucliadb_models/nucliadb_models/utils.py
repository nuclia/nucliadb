import re

import pydantic


class SlugString(pydantic.ConstrainedStr):
    regex = re.compile(r"[a-z0-9_-]+")
