from time import sleep

from rog_client import RogClient

client = RogClient()

def send_and_verify(log_name: str, partition: int, message: str):
    client.connect()
    response = client.send_message(log_name, partition, message)
    expected_response = (0).to_bytes(1, byteorder="big")
    assert response == expected_response


def fetch_and_verify(log_name: str, partition: int, group: str, expected_response: str):
    client.connect()
    response = client.fetch_log(log_name, partition, group)
    success_byte = (0).to_bytes(1, byteorder="big")
    assert response[0:1] == success_byte
    message_size = int.from_bytes(response[1:9], "big")
    assert response[9:(10+message_size)].decode("utf-8") == expected_response

# Test publishing one message to each partition in a log
client.connect()
response = client.create_log("events.log", 10)
expected_response = (0).to_bytes(1, byteorder="big")
assert response == expected_response

for i in range(10):
    send_and_verify("events.log", i, "some data")

    sleep(0.1)

    expected_response = "some data"
    fetch_and_verify("events.log", i, "test-group", expected_response)

# Test publishing more than one message to same partition
send_and_verify("events.log", 0, "first message")
send_and_verify("events.log", 0, "second message")

sleep(0.1)

expected_response = f"first message"
fetch_and_verify("events.log", 0, "test-group", expected_response)

expected_response = f"second message"
fetch_and_verify("events.log", 0, "test-group", expected_response)

# Test fetching data from same partition with different groups
client.connect()
response = client.create_log("other-events.log", 10)
expected_response = (0).to_bytes(1, byteorder="big")
assert response == expected_response

send_and_verify("other-events.log", 0, "first message")
send_and_verify("other-events.log", 0, "second message")

sleep(0.1)

expected_response = f"first message"
fetch_and_verify("other-events.log", 0, "test-group-1", expected_response)

expected_response = f"second message"
fetch_and_verify("other-events.log", 0, "test-group-1", expected_response)

expected_response = f"first message"
fetch_and_verify("other-events.log", 0, "test-group-2", expected_response)

expected_response = f"second message"
fetch_and_verify("other-events.log", 0, "test-group-2", expected_response)

# Fetch data from a log with no data left
client.connect()
response = client.fetch_log("other-events.log", 0, "test-group-2")
assert response[0] == 1
message_size = int.from_bytes(response[1:9], "big")
expected_message = "No data left in the log to be read"
assert response[9:(10+message_size)].decode("utf-8") == expected_message
