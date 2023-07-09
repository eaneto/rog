from time import sleep

from rog_client import RogClient, CRLF

client = RogClient()

def send_and_verify(log_name: str, partition: int, message: str):
    client.connect()
    response = client.send_message(log_name, partition, message)
    expected_response = f"+OK{CRLF}"
    assert response.decode("utf-8") == expected_response


def fetch_and_verify(log_name: str, partition: int, group: str, expected_response: str):
    client.connect()
    response = client.fetch_log(log_name, partition, group)
    assert response.decode("utf-8") == expected_response

# Test publishing one message to each partition in a log
client.connect()
response = client.create_log("events.log", 10)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

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
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

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
