import os
from time import sleep

from rog_client import (
    RogClient,
    create_log_and_check_success,
    fetch_binary_message_and_check_success,
    send_binary_message_and_check_success,
)


def test_send_message_with_5kb():
    client = RogClient()
    log_name = "single-big-message.log"

    create_log_and_check_success(client, log_name, 2)

    data = os.urandom(1024 * 5)

    send_binary_message_and_check_success(client, log_name, 0, data)

    sleep(0.1)

    fetch_binary_message_and_check_success(
        client, log_name, 0, "test-group", data, (1024 * 5 + 9)
    )


def test_send_multiple_messages_with_600kB():
    client = RogClient()
    log_name = "multiple-big-messages.log"

    create_log_and_check_success(client, log_name, 2)

    multiplier = 6
    data = os.urandom(1024 * multiplier)

    for _ in range(10):
        send_binary_message_and_check_success(client, log_name, 0, data)

    sleep(0.1)

    for _ in range(10):
        fetch_binary_message_and_check_success(
            client, log_name, 0, "test-group", data, (1024 * multiplier + 9)
        )
