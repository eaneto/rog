from time import sleep

from addressbook_pb2 import Person
from rog_client import RogClient, CRLF

client = RogClient()

# Send protobuf messages
client.connect()
response = client.create_log("proto-events.log", 10)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

person = Person()
person.id = 1234
person.name = "John Doe"
person.email = "jdoe@example.com"
phone = person.phones.add()
phone.number = "555-4321"
phone.type = Person.PHONE_TYPE_HOME

client.connect()
data = person.SerializeToString()
response = client.send_binary_message("proto-events.log", 0, data)
expected_response = f"+OK{CRLF}"
assert response.decode("utf-8") == expected_response

sleep(0.1)

client.connect()
response = client.fetch_log("proto-events.log", 0, "proto-group")
response_person = Person()
response_person.ParseFromString(response)
assert response_person.id == person.id
assert response_person.name == person.name
assert response_person.email == person.email
assert response_person.phones == person.phones
