from time import sleep

from addressbook_pb2 import Person
from rog_client import RogClient
from random import randint


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


client = RogClient()

# Send one protobuf message
client.connect()
response = client.create_log("proto-events.log", 10)
expected_response = (0).to_bytes(1, byteorder="big")
assert response == expected_response

person = build_person_object()
data = person.SerializeToString()
client.connect()
response = client.send_binary_message("proto-events.log", 0, data)
expected_response = (0).to_bytes(1, byteorder="big")
assert response == expected_response

sleep(0.1)

client.connect()
response = client.fetch_log("proto-events.log", 0, "proto-group")

success_byte = (0).to_bytes(1, byteorder="big")
assert response[0:1] == success_byte
message_size = int.from_bytes(response[1:9], "big")

response_person = Person()
response_person.ParseFromString(response[9:(10+message_size)])
assert response_person.id == person.id
assert response_person.name == person.name
assert response_person.email == person.email
assert response_person.phones == person.phones

# Send multiple protobuf messages to same partition
persons = []
for i in range(25):
    person = build_person_object()
    persons.append(person)
    data = person.SerializeToString()
    client.connect()
    response = client.send_binary_message("proto-events.log", 2, data)
    expected_response = (0).to_bytes(1, byteorder="big")
    assert response == expected_response

sleep(1)

for person in persons:
    client.connect()
    response = client.fetch_log("proto-events.log", 2, "proto-group")

    success_byte = (0).to_bytes(1, byteorder="big")
    assert response[0:1] == success_byte
    message_size = int.from_bytes(response[1:9], "big")

    response_person = Person()
    response_person.ParseFromString(response[9:(10+message_size)])
    assert response_person.id == person.id
    assert response_person.name == person.name
    assert response_person.email == person.email
    assert response_person.phones == person.phones
