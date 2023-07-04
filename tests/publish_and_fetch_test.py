from rog_client import RogClient, CRLF

client = RogClient()

# Test publishing one message to partition
client.connect()
response = client.create_log("events.log", 10)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

for i in range(10):
    client.connect()
    response = client.send_message("events.log", i, "some data")
    expected_response = f"+OK{CRLF}"
    assert response.decode("utf-8") == expected_response

    client.connect()
    response = client.fetch_log("events.log", i, "test-group")
    expected_response = f"some data"
    assert response.decode("utf-8") == expected_response

# Test publishing more than one message to same partition
client.connect()
response = client.send_message("events.log", 0, "first message")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.send_message("events.log", 0, "second message")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.fetch_log("events.log", 0, "test-group")
expected_response = f"first message"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.fetch_log("events.log", 0, "test-group")
expected_response = f"second message"
assert response.decode("utf-8") == expected_response

# Test fetching data from same partition with different groups
client.connect()
response = client.create_log("other-events.log", 10)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.send_message("other-events.log", 0, "first message")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.send_message("other-events.log", 0, "second message")
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.fetch_log("other-events.log", 0, "test-group-1")
expected_response = f"first message"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.fetch_log("other-events.log", 0, "test-group-1")
expected_response = f"second message"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.fetch_log("other-events.log", 0, "test-group-2")
expected_response = f"first message"
assert response.decode("utf-8") == expected_response

client.connect()
response = client.fetch_log("other-events.log", 0, "test-group-2")
expected_response = f"second message"
assert response.decode("utf-8") == expected_response