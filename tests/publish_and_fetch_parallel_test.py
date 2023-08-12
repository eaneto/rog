import string
from multiprocessing import Pool, cpu_count
from random import choices, randint
from time import sleep
from typing import Dict, List

from addressbook_pb2 import Person
from rog_client import RogClient, create_log_and_check_success


def build_random_person_object() -> Person:
    person = Person()
    person.id = randint(1, 100)
    person.name = "".join(
        choices(string.ascii_letters + string.digits, k=randint(10, 25))
    )
    email_name = "".join(
        choices(string.ascii_letters + string.digits, k=randint(5, 15))
    )
    email_at = "".join(choices(string.ascii_letters + string.digits, k=randint(4, 25)))
    person.email = email_name + "@" + email_at + ".com"

    for _ in range(0, randint(1, 10)):
        phone = person.phones.add()
        first_number = str(randint(100, 999))
        second_number = str(randint(1000, 9999))
        phone.number = first_number + "" + second_number
        phone.type = Person.PHONE_TYPE_HOME

    return person


def publish_proto(client: RogClient, person_proto: Person, partition: int):
    data = person_proto.SerializeToString()
    client.connect()
    response = client.send_binary_message("proto-events.log", partition, data)
    expected_response = (0).to_bytes(1, byteorder="big")
    assert response == expected_response


def fetch_all_events_from_partition(client: RogClient, messages, partition):
    for person in messages:
        client.connect()
        response = client.fetch_log("proto-events.log", partition, "proto-group")
        success_byte = (0).to_bytes(1, byteorder="big")
        assert response[0:1] == success_byte
        message_size = int.from_bytes(response[1:9], "big")

        response_person = Person()
        response_person.ParseFromString(response[9 : (10 + message_size)])
        assert response_person.id == person.id
        assert response_person.name == person.name
        assert response_person.email == person.email
        assert response_person.phones == person.phones


def test_publishing_one_message_to_each_partition_on_log_in_parallel():
    client = RogClient()

    create_log_and_check_success(client, "proto-events.log", 10)

    messages_by_partition: Dict[int, List[Person]] = {}
    print(f"Running test with {cpu_count()} processes")
    with Pool(cpu_count()) as pool:
        for i in range(100):
            partition = i % 10
            message = build_random_person_object()

            if partition in messages_by_partition:
                messages_by_partition[partition].append(message)
            else:
                messages_by_partition[partition] = []
                messages_by_partition[partition].append(message)

            pool.apply_async(publish_proto, (message, partition))

        pool.close()
        pool.join()

    sleep(1)

    with Pool(cpu_count()) as pool:
        for partition, messages in messages_by_partition.items():
            pool.apply_async(fetch_all_events_from_partition, (messages, partition))

        pool.close()
        pool.join()
