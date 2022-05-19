from typing import Iterable
from urllib.parse import urlparse


class ExcludeList:

    """Class to exclude certain paths (given as a list of regexes) from tracing requests"""

    def __init__(self, excluded_urls: Iterable[str]):
        self._excluded_urls = excluded_urls

    def url_disabled(self, url: str) -> bool:
        return bool(self._excluded_urls and urlparse(url).path in self._excluded_urls)
