from rog_client import RogClient, create_log_and_check_success


def test_log_creation_with_success():
    client = RogClient()
    create_log_and_check_success(client, "events.log", 10)


# Trying to create a log with existent log name
def test_log_creation_with_existent_log_name():
    client = RogClient()
    create_log_and_check_success(client, "events.log", 10)

    client.connect()
    response = client.create_log("events.log", 10)
    assert response[0] == 1
    message_size = int.from_bytes(response[1:9], "big")
    expected_message = f"Log events.log already exists"
    assert response[9 : (10 + message_size)].decode("utf-8") == expected_message


# Trying to create a log with 0 partitions
def test_log_creation_with_invalid_number_of_partitions():
    client = RogClient()
    client.connect()
    response = client.create_log("other-events.log", 0)
    assert response[0] == 1
    message_size = int.from_bytes(response[1:9], "big")
    expected_message = f"Number of partitions must be at least 1"
    assert response[9 : (10 + message_size)].decode("utf-8") == expected_message
