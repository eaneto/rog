from rog_client import RogClient, CRLF

client = RogClient()

client.connect()
response = client.create_log("events.log", 10)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

# Trying to create a log with existent log name
client.connect()
response = client.create_log("events.log", 10)
expected_response = f"-Log events.log already exists{CRLF}"
assert response.decode("utf-8") == expected_response

# Trying to create a log with 0 partitions
client.connect()
response = client.create_log("other-events.log", 0)
expected_response = f"-Number of partitions must be at least 1{CRLF}"
assert response.decode("utf-8") == expected_response
