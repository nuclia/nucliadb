# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import subprocess
import time
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest


@pytest.fixture
def database_url() -> Iterator[str]:
    name = f"nidx-lpg-test-{uuid4().hex}"
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            name,
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "-p",
            "127.0.0.1::5432",
            "postgres:16-alpine",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    try:
        port = (
            subprocess.run(
                ["docker", "port", name, "5432/tcp"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
            .stdout.strip()
            .rsplit(":", 1)[1]
        )
        url = f"postgresql://postgres@127.0.0.1:{port}/postgres"
        deadline = time.monotonic() + 30
        while True:
            try:
                with psycopg.connect(url, connect_timeout=1) as connection:
                    connection.execute("SELECT 1")
                break
            except psycopg.OperationalError as error:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Dedicated LPG test PostgreSQL did not become ready") from error
                time.sleep(0.1)
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=True, capture_output=True, timeout=15)
