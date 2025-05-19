# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import nucliadb_sdk
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_models.security import RequestSecurity, ResourceSecurity

CLASSIFIED_INFO = [
    """El presidente del Gobierno, Pedro Sánchez, ha anunciado este viernes \
    que el Consejo de Ministros aprobará el próximo martes la declaración del \
    estado de alarma para todo el territorio nacional, que se prolongará durante \
    seis meses, hasta el 9 de mayo de 2021, para hacer frente a la segunda ola de \
    la pandemia de coronavirus.""",
    """
    The president of the united states of America, Donald Trump, has announced \
    that he will not accept the results of the elections, and that he will \
    continue to be the president of the united states of America.
    """,
]

PUBLIC_NEWS = [
    "La temperatura hoy en Sevilla será de 20 grados centígrados como máxima y 10 grados centígrados como mínima.",
    "El Guggenheim de Bilbao ha sido galardonado con el premio Príncipe de Asturias de las Artes.",
]


def test_security_search(sdk: nucliadb_sdk.NucliaDB, kb: KnowledgeBoxObj):
    legal_group = "legal"
    sales_group = "sales"
    # Create some classified resources that only legal should have access to
    for i, text in enumerate(CLASSIFIED_INFO):
        sdk.create_resource(
            kbid=kb.uuid,
            title=f"Classified {i}",
            summary=text,
            security=ResourceSecurity(access_groups=[legal_group]),
        )

    # Create some public news resources
    for i, text in enumerate(PUBLIC_NEWS):
        sdk.create_resource(kbid=kb.uuid, title=f"News {i}", summary=text, security=None)

    # Only legal group has access to any classified info
    results = sdk.find(kbid=kb.uuid, query="Classified", security=RequestSecurity(groups=[sales_group]))
    assert len(results.resources) == 0

    results = sdk.find(kbid=kb.uuid, query="Classified", security=RequestSecurity(groups=[legal_group]))
    assert len(results.resources) == 2

    # Public news are accessible to everyone
    results = sdk.find(kbid=kb.uuid, query="News", security=RequestSecurity(groups=[sales_group]))
    assert len(results.resources) == 2

    results = sdk.find(kbid=kb.uuid, query="News", security=RequestSecurity(groups=[legal_group]))
    assert len(results.resources) == 2

    # Querying without security should return all resources
    results = sdk.find(kbid=kb.uuid, query="Classified OR News")
    assert len(results.resources) == 4
