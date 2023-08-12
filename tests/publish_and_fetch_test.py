from time import sleep

from rog_client import (
    RogClient,
    create_log_and_check_success,
    fetch_message_and_check_success,
    send_message_and_check_success,
)


def test_publish_one_message_to_each_partition():
    client = RogClient()

    create_log_and_check_success(client, "events.log", 10)

    for i in range(10):
        send_message_and_check_success(client, "events.log", i, "some data")

        sleep(0.1)

        expected_response = "some data"
        fetch_message_and_check_success(
            client, "events.log", i, "test-group", expected_response
        )


def test_publishing_more_than_one_message_to_same_partitions():
    client = RogClient()
    create_log_and_check_success(client, "events.log", 10)
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


def test_fetching_data_from_same_partition_with_differente_groups():
    client = RogClient()
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


def test_fetch_data_from_a_log_with_no_data_left():
    client = RogClient()
    create_log_and_check_success(client, "other-events.log", 10)
    client.connect()
    response = client.fetch_log("other-events.log", 0, "test-group-2")
    assert response[0] == 1
    message_size = int.from_bytes(response[1:9], "big")
    expected_message = "No data left in the log to be read"
    assert response[9 : (10 + message_size)].decode("utf-8") == expected_message
