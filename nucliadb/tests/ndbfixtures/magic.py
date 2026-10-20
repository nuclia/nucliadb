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

import logging

import pytest
from pytest import FixtureRequest, Metafunc
from pytest_lazy_fixtures import lf

logger = logging.getLogger("fixtures.magic")


DEPLOY_MODE_MARK_NAME = "deploy_modes"

DEPLOY_MODE_FIXTURES = {
    "nucliadb_search": [
        "cluster",
        "standalone",
    ],
    "nucliadb_reader": [
        "component",
        "standalone",
    ],
    "nucliadb_writer": [
        "component",
        "standalone",
    ],
    "nucliadb_train": [
        "standalone",
    ],
    "nucliadb_train_grpc": [
        "component",
        "standalone",
    ],
    "nucliadb_ingest_grpc": [
        "component",
        "standalone",
    ],
}


class MagicFixturesError(Exception): ...


def pytest_configure(config):
    """Register the marker"""
    config.addinivalue_line(
        "markers",
        f"{DEPLOY_MODE_MARK_NAME}(modes): specify one ore more nucliadb deployment modes to run the test with",
    )


def pytest_generate_tests(metafunc: Metafunc):
    """This pytest hook allows customization of each test function defined in
    our test suite. We use it to modify tests and change some specific fixtures.

    With a custom pytest mark, we customize which deployment we want to utilize
    for each test. We replace a defined fixture name like `nucliadb_reader` for
    one of the deployment fixtures (e.g. `component_nucliadb_reader`). This
    replacement allows multiple advantages:
    - use a common `nucliadb_reader` fixture for all kind of deployments
    - lazily call dependent fixtures

    Furthemore, as we are not actually replacing fixtures but parametrizing its
    name, we can mark multiple deployment modes an run tests multiple times, one
    per specified mode
    """
    for mark in metafunc.definition.own_markers:
        if mark.name == DEPLOY_MODE_MARK_NAME:
            # this fixtures is parametrized with an specific deployment mode
            for fixture_name in metafunc.fixturenames:
                if fixture_name in DEPLOY_MODE_FIXTURES:
                    argvalues = []
                    for deploy_mode in mark.args:
                        if deploy_mode in DEPLOY_MODE_FIXTURES[fixture_name]:
                            argvalues.append(lf(f"{deploy_mode}_{fixture_name}"))
                        else:
                            raise MagicFixturesError(
                                f"Requesting fixture {fixture_name} with an unavailable mode: {deploy_mode}. "
                                "Did you forgot to add it in the list of magic fixtures?"
                            )
                    metafunc.parametrize(fixture_name, argvalues, indirect=True)


# HACK we need to define placeholder fixtures that prevent collection failures
# and return the value of the injected lazy fixture


async def _generic_injected_fixture(request: FixtureRequest):
    try:
        yield request.param
    except AttributeError as exc:
        raise MagicFixturesError(
            "Are you using a magic fixture without the deploy_modes decorator?"
        ) from exc


nucliadb_search = pytest.fixture(_generic_injected_fixture, scope="function")
nucliadb_reader = pytest.fixture(_generic_injected_fixture, scope="function")
nucliadb_writer = pytest.fixture(_generic_injected_fixture, scope="function")
nucliadb_train = pytest.fixture(_generic_injected_fixture, scope="function")
nucliadb_train_grpc = pytest.fixture(_generic_injected_fixture, scope="function")
nucliadb_ingest_grpc = pytest.fixture(_generic_injected_fixture, scope="function")
