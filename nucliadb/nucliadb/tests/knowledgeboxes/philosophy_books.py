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

import pytest
from httpx import AsyncClient


@pytest.fixture(scope="function")
async def philosophy_books_kb(
    nucliadb_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    payloads = [
        {
            "slug": "meditations",
            "title": "Meditations",
            "summary": (
                "Series of personal writings by Marcus Aurelius recording his private notes to "
                "himself and ideas on Stoic philosophy"
            ),
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "Marcus Aurelius",
                            "group": "PERSON",
                        },
                    }
                ]
            },
        },
        {
            "slug": "nicomachean-ethics",
            "title": "Nicomachean Ethics",
            "summary": (
                "Aristotle's best-known work on ethics, the science of human life"
            ),
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "Aristotle",
                            "group": "PERSON",
                        },
                    }
                ]
            },
        },
        {
            "slug": "beyond-good-and-evil",
            "title": "Beyond Good and Evil: Prelude to a philosophy of the Future",
            "summary": (
                "Nietzsche acuse past philosophers of lacking critical sense and blindly accepting "
                "dogmatic premises in their consideration of morality"
            ),
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "Friedrich Nietzsche",
                            "group": "PERSON",
                        },
                    }
                ]
            },
        },
        {
            "slug": "meditations-on-first-philosophy",
            "title": "Meditations on First Philosophy",
            "summary": (
                "Six meditations in which Descartes first discards all belief in things that are "
                "not absolutely certain, and then tries to establish what can be known for sure"
            ),
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "René Descartes",
                            "group": "PERSON",
                        },
                    }
                ]
            },
        },
        {
            "slug": "the-principles-of-the-most-ancient-and-modern-philosophy",
            "title": "The Principles of the most Ancient and Modern Philosophy",
            "summary": (
                "Conway's monistic view of the world as created from one substance"
            ),
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "Anne Conway",
                            "group": "PERSON",
                        },
                    }
                ]
            },
        },
        {
            "slug": "ethics",
            "title": "Ethics",
            "summary": (
                "Spinoza puts forward a small number of definitions and axioms from which he "
                "attempts to derive hundreds of propositions and corollaries"
            ),
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "Baruch Spinoza",
                            "group": "PERSON",
                        },
                    }
                ]
            },
        },
        {
            "slug": "critique-of-pure-reason",
            "title": "Critique of Pure Reason",
            "summary": ("Kant seeks to determine the limits and scope of metaphysics"),
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "Immanuel Kant",
                            "group": "PERSON",
                        },
                    }
                ]
            },
        },
        {
            "slug": "the-human-condition",
            "title": "The Human Condition",
            "summary": (
                "Arendt differentiates political and social concepts, labor and work, and various "
                "forms of actions; she then explores the implications of those distinctions"
            ),
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "Hannah Arendt",
                            "group": "PERSON",
                        },
                    }
                ]
            },
        },
    ]

    resp = await nucliadb_manager.post("/kbs", json={"slug": "philosophy-books"})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    for payload in payloads:
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            headers={"X-Synchronous": "true"},
            json=payload,
        )
        assert resp.status_code == 201

    yield kbid

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200
