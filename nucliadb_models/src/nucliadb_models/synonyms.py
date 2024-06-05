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
from typing import Dict, List, Type, TypeVar

from pydantic import BaseModel

from nucliadb_protos import knowledgebox_pb2

_T = TypeVar("_T")


# Shared classes


class KnowledgeBoxSynonyms(BaseModel):
    synonyms: Dict[str, List[str]]

    @classmethod
    def from_message(cls: Type[_T], message: knowledgebox_pb2.Synonyms) -> _T:
        return cls(
            **dict(
                synonyms={
                    term: list(term_synonyms.synonyms)
                    for term, term_synonyms in message.terms.items()
                }
            )
        )

    def to_message(self) -> knowledgebox_pb2.Synonyms:
        pbsyn = knowledgebox_pb2.Synonyms()
        for term, term_synonyms in self.synonyms.items():
            pbsyn.terms[term].synonyms.extend(term_synonyms)
        return pbsyn


# Visualization classes (Those used on reader endpoints)
# Creation and update classes (Those used on writer endpoints)
# - This model is unified

# Processing classes (Those used to sent to push endpoints)
# - Doesn't apply, Synonyms are not sent to process
