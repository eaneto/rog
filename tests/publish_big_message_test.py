import os
from time import sleep

from rog_client import (
    RogClient,
    create_log_and_check_success,
    fetch_binary_message_and_check_success,
    send_binary_message_and_check_success,
)

log_name = "big-packets.log"
data = os.urandom(1024 * 5)


def test_send_message_with_5kb():
    client = RogClient()

    create_log_and_check_success(client, log_name, 2)

    send_binary_message_and_check_success(client, log_name, 0, data)

    sleep(0.1)

    fetch_binary_message_and_check_success(
        client, log_name, 0, "test-group", data, 5150
    )


# Send multiple big messages
def test_send_multiple_messages_over_5kb():
    client = RogClient()
    create_log_and_check_success(client, log_name, 2)
    for _ in range(10):
        send_binary_message_and_check_success(client, log_name, 0, data)

        sleep(0.1)

    for _ in range(10):
        fetch_binary_message_and_check_success(
            client, log_name, 0, "test-group", data, 5150
        )
