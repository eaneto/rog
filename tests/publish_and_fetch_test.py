from time import sleep

from rog_client import (
    RogClient,
    create_log_and_check_success,
    fetch_message_and_check_success,
    send_message_and_check_success,
)

client = RogClient()

# Test publishing one message to each partition in a log
create_log_and_check_success(client, "events.log", 10)

for i in range(10):
    send_message_and_check_success(client, "events.log", i, "some data")

    sleep(0.1)

    expected_response = "some data"
    fetch_message_and_check_success(
        client, "events.log", i, "test-group", expected_response
    )

# Test publishing more than one message to same partition
send_message_and_check_success(client, "events.log", 0, "first message")
send_message_and_check_success(client, "events.log", 0, "second message")

sleep(0.1)

expected_response = f"first message"
fetch_message_and_check_success(
    client, "events.log", 0, "test-group", expected_response
)

expected_response = f"second message"
fetch_message_and_check_success(
    client, "events.log", 0, "test-group", expected_response
)

# Test fetching data from same partition with different groups
create_log_and_check_success(client, "other-events.log", 10)

send_message_and_check_success(client, "other-events.log", 0, "first message")
send_message_and_check_success(client, "other-events.log", 0, "second message")

sleep(0.1)

expected_response = f"first message"
fetch_message_and_check_success(
    client, "other-events.log", 0, "test-group-1", expected_response
)

expected_response = f"second message"
fetch_message_and_check_success(
    client, "other-events.log", 0, "test-group-1", expected_response
)

expected_response = f"first message"
fetch_message_and_check_success(
    client, "other-events.log", 0, "test-group-2", expected_response
)

expected_response = f"second message"
fetch_message_and_check_success(
    client, "other-events.log", 0, "test-group-2", expected_response
)

# Fetch data from a log with no data left
client.connect()
response = client.fetch_log("other-events.log", 0, "test-group-2")
assert response[0] == 1
message_size = int.from_bytes(response[1:9], "big")
expected_message = "No data left in the log to be read"
assert response[9 : (10 + message_size)].decode("utf-8") == expected_message
