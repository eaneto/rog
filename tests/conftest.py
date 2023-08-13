import pytest

from rog_setup import kill_rog_server, setup_rog_server


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="local")


@pytest.fixture(autouse=True)
def setup_and_teardown_rog_server(request):
    pid = setup_rog_server(request)
    yield
    kill_rog_server(pid)
