import os
import signal
from time import sleep

import psutil

from rog_client import (
    RogClient,
    ack_message_and_check_success,
    create_log_and_check_success,
    fetch_message_and_check_success,
    send_message_and_check_success,
)
from rog_setup import initialize_rog_server, kill_rog_server


def find_and_kill_rog_server():
    processes = psutil.process_iter()
    name = "rog-server"
    ssache_process = [p for p in processes if name in p.name()][0]
    os.kill(ssache_process.pid, signal.SIGTERM)


def test_log_loading_after_server_restart(request):
    client = RogClient()
    log_name = "restart.log"

    create_log_and_check_success(client, log_name, 10)

    message = "random message"
    send_message_and_check_success(client, log_name, 0, message)

    sleep(0.1)

    find_and_kill_rog_server()

    profile = request.config.getoption("--profile")
    pid = initialize_rog_server(profile)

    fetch_message_and_check_success(client, log_name, 0, "test-group", message)

    ack_message_and_check_success(client, log_name, 0, "test-group")

    kill_rog_server(pid)
