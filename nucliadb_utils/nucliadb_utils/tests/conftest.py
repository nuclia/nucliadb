pytest_plugins = [
    "pytest_docker_fixtures",
    "nucliadb_utils.tests.gcs",
    "nucliadb_utils.tests.nats",
    "nucliadb_utils.tests.s3",
    "nucliadb_utils.tests.local",
]
