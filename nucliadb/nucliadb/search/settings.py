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
#
from typing import List, Optional

from pydantic import BaseSettings

from nucliadb_models import predict as predict_models


class Settings(BaseSettings):
    driver: str = "redis"  # redis | tikv
    driver_redis_url: Optional[str] = None
    driver_tikv_url: Optional[List[str]] = []

    nodes_load_ingest: bool = False

    search_timeout: float = 10.0


settings = Settings()


class PredictModelSettings(BaseSettings):
    """
    Settings related to predict engine.

    These settings enable someone running predict in standalone mode to
    override the models used by the predict engine.
    """

    predict_generative_model: Optional[predict_models.GenerativeModel] = None
    predict_anonymization_model: Optional[predict_models.AnonimizationModel] = None
    predict_semantic_model: Optional[predict_models.SemanticModel] = None
    predict_ner_model: Optional[predict_models.NERModel] = None
