import os
from time import sleep

from rog_client import (
    RogClient,
    create_log_and_check_success,
    fetch_binary_message_and_check_success,
    send_binary_message_and_check_success,
)

# Sends a message with 5kB
client = RogClient()
log_name = "big-packets.log"
data = os.urandom(1024 * 5)

create_log_and_check_success(client, log_name, 2)

# Send one big message
send_binary_message_and_check_success(client, log_name, 0, data)

sleep(0.1)

fetch_binary_message_and_check_success(client, log_name, 0, "test-group", data, 5150)

# Send multiple big messages
for i in range(10):
    send_binary_message_and_check_success(client, log_name, 0, data)

sleep(0.1)

for i in range(10):
    fetch_binary_message_and_check_success(
        client, log_name, 0, "test-group", data, 5150
    )
