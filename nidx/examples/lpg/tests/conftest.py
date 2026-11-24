# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import json
import os
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import grpc
import psycopg
import pytest


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def lpg_address(database_url: str, tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    root = Path(__file__).resolve().parents[4]
    build = subprocess.run(
        [
            "cargo",
            "build",
            "--manifest-path",
            str(root / "nidx/Cargo.toml"),
            "--no-default-features",
            "--example",
            "lpg_test_server",
            "--message-format=json",
        ],
        env={**os.environ, "SQLX_OFFLINE": "true"},
        check=True,
        capture_output=True,
        text=True,
        timeout=600,
    )
    executable = next(
        message["executable"]
        for line in build.stdout.splitlines()
        if (message := json.loads(line)).get("reason") == "compiler-artifact"
        and message["target"]["name"] == "lpg_test_server"
        and message.get("executable")
    )
    directory = tmp_path_factory.mktemp("lpg-server")
    ready = directory / "ready"
    log_path = directory / "nidx.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [executable, str(ready)],
            env={**os.environ, "LPG_TEST_DATABASE_URL": database_url},
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            while not ready.exists():
                if process.poll() is not None:
                    raise RuntimeError(f"LPG test server failed:\n{log_path.read_text()}")
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"LPG test server did not start:\n{log_path.read_text()}")
                time.sleep(0.1)
            address = ready.read_text()
            with grpc.insecure_channel(address) as channel:
                grpc.channel_ready_future(channel).result(timeout=10)
            yield address
        finally:
            assert process.stdin is not None
            process.stdin.close()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise TimeoutError(f"LPG test server did not stop:\n{log_path.read_text()}")
            if process.returncode:
                raise RuntimeError(
                    f"LPG test server exited with {process.returncode}:\n{log_path.read_text()}"
                )
