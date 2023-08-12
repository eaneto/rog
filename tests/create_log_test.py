from rog_client import RogClient

client = RogClient()

client.connect()
response = client.create_log("events.log", 10)
expected_response = (0).to_bytes(1, byteorder="big")
assert response == expected_response

# Trying to create a log with existent log name
client.connect()
response = client.create_log("events.log", 10)
assert response[0] == 1
message_size = int.from_bytes(response[1:9], "big")
expected_message = f"Log events.log already exists"
assert response[9:(10+message_size)].decode("utf-8") == expected_message

# Trying to create a log with 0 partitions
client.connect()
response = client.create_log("other-events.log", 0)
assert response[0] == 1
message_size = int.from_bytes(response[1:9], "big")
expected_message = f"Number of partitions must be at least 1"
assert response[9:(10+message_size)].decode("utf-8") == expected_message
