import pytest

from rog_setup import kill_rog_server, setup_rog_server


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="local")
    parser.addoption("--setup-server", action="store", default="true")


@pytest.fixture(autouse=True)
def setup_and_teardown_rog_server(request):
    if request.config.getoption("--setup-server") == "true":
        pid = setup_rog_server(request)
        yield
        kill_rog_server(pid)
    else:
        yield
