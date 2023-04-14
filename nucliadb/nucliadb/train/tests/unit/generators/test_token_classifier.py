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
from collections import OrderedDict

import pytest

from nucliadb.train.generators.token_classifier import process_entities


@pytest.mark.parametrize(
    "text,ners,paragraphs,expected_segments",
    [
        (
            " \n The Legend of Zelda es una serie de videojuegos de acción-aventura \n desarrollada por Nintendo y creada por el diseñador de juegos \n japonés Shigeru Miyamoto. La serie de videojuegos The Legend of \n Zelda se ha convertido en una franquicia icónica y popular en el \n mundo de los videojuegos desde su lanzamiento inicial en 1986. \n \n Los juegos de The Legend of Zelda siguen la historia de un joven \n llamado Link, quien se embarca en una búsqueda épica para rescatar a \n la princesa Zelda y derrotar al malvado antagonista, Ganon, en un \n mundo de fantasía conocido como Hyrule. A lo largo de la serie, los \n jugadores exploran vastos mundos abiertos, resuelven acertijos, \n completan mazmorras, obtienen poderosas armas y objetos, y luchan \n contra enemigos variados mientras buscan la Trifuerza, un artefacto \n mágico que concede deseos. \n \n Cada juego de The Legend of Zelda presenta su propia historia, \n personajes y mecánicas de juego únicas, aunque todos comparten \n elementos comunes, como la lucha contra enemigos con espadas, la \n resolución de acertijos y la exploración de un mundo abierto. La \n serie ha sido aclamada por su jugabilidad innovadora, su narrativa \n envolvente y su música icónica, y ha sido elogiada por su atención \n al detalle y su rica historia. The Legend of Zelda ha sido lanzada \n en varias consolas de Nintendo, incluyendo la NES, SNES, Nintendo \n 64, GameCube, Wii, Wii U, Nintendo 3DS y Nintendo Switch, y ha \n vendido millones de copias en todo el mundo, convirtiéndose en una \n de las franquicias de videojuegos más exitosas y duraderas de la \n historia. \n \n \n ",
            OrderedDict(
                [
                    ((144, 160), ("PERSON", "Shigeru Miyamoto")),
                    ((411, 415), ("PERSON", "Link")),
                    ((486, 491), ("PERSON", "Zelda")),
                    ((527, 532), ("PERSON", "Ganon")),
                    ((780, 786), ("PERSON", "buscan")),
                    ((790, 799), ("PERSON", "Trifuerza")),
                ]
            ),
            [(0, 333), (333, 844), (844, 1598)],
            [[("Zelda", "B-GAME_GENRE")]],
        ),
    ],
)
def test_process_entities(text, ners, paragraphs, expected_segments):
    breakpoint()
    segments = list(process_entities(text, ners, paragraphs))
    for segment in segments:
        print(segment)
