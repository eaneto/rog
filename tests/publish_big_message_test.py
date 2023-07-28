from time import sleep
from rog_client import RogClient, CRLF

# Sends a message with 5kB
client = RogClient()
with open("tests/big_packet.txt") as fp:
    data = fp.read()

log_name = "big-packets.log"

def send_big_message():
    client.connect()
    response = client.send_message(log_name, 0, data)
    expected_response = f"+OK{CRLF}"
    assert response.decode("utf-8") == expected_response

client.connect()
response = client.create_log(log_name, 2)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

# Send one big message
send_big_message()

sleep(0.1)

client.connect()
response = client.fetch_log(log_name, 0, "test-group", 5126)
assert response.decode("utf-8") == data

# Send multiple big messages
for i in range(10):
    send_big_message()

sleep(0.1)

for i in range(10):
    client.connect()
    response = client.fetch_log(log_name, 0, "test-group", 5126)
    assert response.decode("utf-8") == data
