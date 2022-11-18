import pytest


def mock_murmur3_32(key, seed=0):
    return 4294967295


@pytest.fixture(scope="function")
def clandestined(mocker):
    mocker.patch("mmh3.hash", return_value=4294967295)
