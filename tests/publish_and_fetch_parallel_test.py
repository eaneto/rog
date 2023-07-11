import string
from typing import Dict, List

from addressbook_pb2 import Person
from multiprocessing import Pool, cpu_count
from time import sleep
from random import randint, choices

from rog_client import RogClient, CRLF

client = RogClient()


def build_random_person_object() -> Person:
    person = Person()
    person.id = randint(1, 100)
    person.name = ''.join(choices(string.ascii_letters + string.digits, k=randint(10, 25)))
    email_name = ''.join(choices(string.ascii_letters + string.digits, k=randint(5, 15)))
    email_at = ''.join(choices(string.ascii_letters + string.digits, k=randint(4, 25)))
    person.email = email_name + "@" + email_at + ".com"

    for _ in range(0, randint(1, 10)):
        phone = person.phones.add()
        first_number = str(randint(100, 999))
        second_number = str(randint(1000, 9999))
        phone.number = first_number + "" + second_number
        phone.type = Person.PHONE_TYPE_HOME

    return person


def publish_proto(person_proto: Person, partition: int):
    data = person_proto.SerializeToString()
    client.connect()
    response = client.send_binary_message("proto-events.log", partition, data)
    expected_response = f"+OK{CRLF}"
    assert response.decode("utf-8") == expected_response


# Test publishing one message to each partition in a log
client.connect()
response = client.create_log("proto-events.log", 10)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

messages_by_partition: Dict[int, List[Person]] = {}
print(f"Running test with {cpu_count()} processes")
with Pool(cpu_count()) as pool:
    for i in range(30):
        partition = i % 10
        message = build_random_person_object()

        if partition in messages_by_partition:
            messages_by_partition[partition].append(message)
        else:
            messages_by_partition[partition] = []
            messages_by_partition[partition].append(message)

        pool.apply(publish_proto, (message, partition))

sleep(1)


for partition, messages in messages_by_partition.items():
    for person in messages:
        client.connect()
        response = client.fetch_log("proto-events.log", partition, "proto-group")
        response_person = Person()
        response_person.ParseFromString(response)
        assert response_person.id == person.id
        assert response_person.name == person.name
        assert response_person.email == person.email
        assert response_person.phones == person.phones
