from random import randint
from time import sleep

from addressbook_pb2 import Person
from rog_client import RogClient, create_log_and_check_success


def build_person_object():
    person = Person()
    person.id = randint(1, 100)
    person.name = "John Doe"
    person.email = "jdoe@example.com"
    for _ in range(0, randint(1, 10)):
        phone = person.phones.add()
        phone.number = "555-4321"
        phone.type = Person.PHONE_TYPE_HOME
        return person


def test_send_one_protobuf_message():
    client = RogClient()

    log_name = "single-proto-event.log"
    create_log_and_check_success(client, log_name, 10)

    person = build_person_object()
    data = person.SerializeToString()
    client.connect()
    response = client.send_binary_message(log_name, 0, data)
    expected_response = (0).to_bytes(1, byteorder="big")
    assert response == expected_response

    sleep(0.1)

    client.connect()
    response = client.fetch_log(log_name, 0, "proto-group")

    success_byte = (0).to_bytes(1, byteorder="big")
    assert response[0:1] == success_byte
    message_size = int.from_bytes(response[1:9], "big")

    response_person = Person()
    response_person.ParseFromString(response[9 : (10 + message_size)])
    assert response_person.id == person.id
    assert response_person.name == person.name
    assert response_person.email == person.email
    assert response_person.phones == person.phones


def test_send_multiple_protobuf_messages_to_same_partition():
    client = RogClient()
    log_name = "multiple-proto-events.log"

    create_log_and_check_success(client, log_name, 10)

    persons = []
    for i in range(25):
        person = build_person_object()
        persons.append(person)
        data = person.SerializeToString()
        client.connect()
        response = client.send_binary_message(log_name, 2, data)
        expected_response = (0).to_bytes(1, byteorder="big")
        assert response == expected_response

    sleep(1)

    for person in persons:
        client.connect()
        response = client.fetch_log(log_name, 2, "proto-group")

        success_byte = (0).to_bytes(1, byteorder="big")
        assert response[0:1] == success_byte
        message_size = int.from_bytes(response[1:9], "big")

        response_person = Person()
        response_person.ParseFromString(response[9 : (10 + message_size)])
        assert response_person.id == person.id
        assert response_person.name == person.name
        assert response_person.email == person.email
        assert response_person.phones == person.phones
