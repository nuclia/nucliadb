# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from typing import Optional

try:
    from pydantic.v1 import BaseSettings
except ImportError:
    from pydantic import BaseSettings


class Settings(BaseSettings):
    main_api: str = "http://localhost:8080/api"
    search_api: Optional[str] = None
    reader_api: Optional[str] = None
    predict_api: str = (
        "http://predict.learning.svc.cluster.local:8080/api/internal/predict"
    )
    benchmark_output: Optional[str] = None
    saved_requests_file: Optional[str] = None
    exports_folder: str = "exports"


def get_benchmark_output_file():
    return settings.benchmark_output or "benchmark.json"


def get_search_api_url():
    return settings.search_api or settings.main_api


def get_reader_api_url():
    return settings.reader_api or settings.main_api


def get_predict_api_url():
    return settings.predict_api


settings = Settings()
