from time import sleep
from rog_client import (
    RogClient,
    create_log_and_check_success,
    fetch_message_and_check_success,
    send_message_and_check_success,
)

# Sends a message with 5kB
client = RogClient()
with open("tests/big_packet.txt") as fp:
    data = fp.read()

log_name = "big-packets.log"

create_log_and_check_success(client, log_name, 2)

# Send one big message
send_message_and_check_success(client, log_name, 0, data)

sleep(0.1)

client.connect()
response = client.fetch_log(log_name, 0, "test-group", 5150)
success_byte = (0).to_bytes(1, byteorder="big")
assert response[0:1] == success_byte
message_size = int.from_bytes(response[1:9], "big")
assert response[9 : (10 + message_size)].decode("utf-8") == data

# Send multiple big messages
for i in range(10):
    send_message_and_check_success(client, log_name, 0, data)

sleep(0.1)

for i in range(10):
    fetch_message_and_check_success(client, log_name, 0, "test-group", data, 5150)
