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
        sdk.create_resource(
            kbid=kb.uuid, title=f"News {i}", summary=text, security=None
        )

    # Only legal group has access to any classified info
    results = sdk.find(
        kbid=kb.uuid, query="Classified", security=RequestSecurity(groups=[sales_group])
    )
    assert len(results.resources) == 0

    results = sdk.find(
        kbid=kb.uuid, query="Classified", security=RequestSecurity(groups=[legal_group])
    )
    assert len(results.resources) == 2

    # Public news are accessible to everyone
    results = sdk.find(
        kbid=kb.uuid, query="News", security=RequestSecurity(groups=[sales_group])
    )
    assert len(results.resources) == 2

    results = sdk.find(
        kbid=kb.uuid, query="News", security=RequestSecurity(groups=[legal_group])
    )
    assert len(results.resources) == 2

    # Querying without security should return all resources
    results = sdk.find(kbid=kb.uuid, query="Classified OR News")
    assert len(results.resources) == 4
