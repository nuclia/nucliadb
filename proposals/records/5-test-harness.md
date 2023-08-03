# NucliaDB Test Harness

Let's create a global test harness framework for NucliaDB that can be used by the CI and for local development.

## Current solution/situation

Testing the different components of NucliaDB in unit tests and integration tests involves setting
up and starting co-services, preparing data and tearing down everything.

The project uses `pytest` for this, along with a few `Makefile` files and some
other tools.

We have fixtures and Makefile files scattered around each component that have a bit of duplication
and sometime slighlty different implementations.

This leads to potential tech debt across the monorepo. For instance, if one day we decide
to move away from `flake8` and use `ruff`, we will need to change every `Makefile` and pip requirements.

Developers looking to build a new unit or integration test can probably reuse existing bits
to get started -- but they need to investigate across the monorepo to find those gems.

We also don't always leverage our own libs. For instance, in integration tests, some calls
are made using a raw HTTP client like `httpx` where they could use our own `sdk` or CLI.

For debugging, we also have flags like `RUST_LOG` that can be used to understand what's going on
when a test fails. These flags are not part of test runners in `Makefile` files.

## Proposed Solution

The next logical step to improve the situation is to start building in our monorepo
a test harness framework that would replace all our `Makefile` files and consolidate our
fixtures and helpers.

Let's add a `nucliadb_test` Python package with 3 elements:

- a test harness CLI, called `nucliadb-test`
- our reusable pytest fixtures
- test helpers

The test harness will come as a single CLI command called `nucliadb-test`, and implement
different actions that will be simple wrappers and can be called from the root of the repo
or in any package.

Examples:

- `nucliadb-test format` : formats the Python and Rust code.
- `nucliadb-test test --debug`: runs all the tests in debug mode
- `nucliadb-test test --python`: run only the Python tests
- etc.

The `nucliadb_test` will also consolidate tests fixtures and any test helper that we might need.
For example, we could provide a helper that creates a shard and instanciates
a `NucliaDBClient` to play with it.

## Rollout plan

[Describe implementation plan]

## Success Criteria

[How will we know if the proposal was successful?]
