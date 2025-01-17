# Rog

A simple messaging system inspired by [Kafka][0].

## Building

To build both the cli and the server you can just run `cargo build
--release`.

## Integration tests

The integration tests are written in python with [pytest][1]. You can
install the dependencies with pip: `pip install -r
tests/requirements.txt`. The tests use protobuf for a few scenarios,
so you need the protobuf compiler, you can generated the necessary
files running this command:

```shell
protoc -I=tests/proto --python_out=tests tests/proto/addressbook.proto
```

And to execute all tests you can run:

```shell
pytest tests
```

If you don't want to install protoc you can run the tests with the
local docker image, this image is used simply to run this tests in an
isolated environment.

```shell
docker buildx build .
```

## Rog CLI

To interact with the server you can use the rog cli.

```
Usage: rog-cli --address <ADDRESS> <COMMAND>

Commands:
  create-log
  publish
  fetch
  help        Print this message or the help of the given subcommand(s)

Options:
  -a, --address <ADDRESS>  Rog server address on the format ip:port, e.g.: 127.0.0.1:7878
  -h, --help               Print help
```

## Protocol specification

The Rog protocol is implemented with TCP. In all commands the first
byte is the command byte, this is used to identify which command is
being sent. There are two client implementations in this repository:
one in rust, [for the cli](./src/bin/cli.rs) and one in python used in
the [integration tests](./tests/rog_client.py).

### Commands

#### Create log

| Field         | Type   | Description                               |
|---------------|--------|-------------------------------------------|
| Command byte  | u8     | Fixed value for the create log command, 0 |
| partitions    | u8     | Number of partitions for the log          |
| Log name size | u8     | Size of the log name in bytes             |
| Log name      | String | Log name                                  |

##### Successful response

| Field        | Type | Description                                        |
|--------------|------|----------------------------------------------------|
| Success byte | u8   | Indicates if the response was successful or not, 0 |

#### Publish

| Field         | Type   | Description                            |
|---------------|--------|----------------------------------------|
| Command byte  | u8     | Fixed value for the publish command, 1 |
| partition     | u8     | Partitions to publish the message      |
| Log name size | u8     | Size of the log name in bytes          |
| Log name      | String | Log name                               |
| Data size     | u64    | Size of the message in bytes           |
| Data          | bytes  | The actual content of the message      |

##### Successful response

| Field        | Type | Description                                        |
|--------------|------|----------------------------------------------------|
| Success byte | u8   | Indicates if the response was successful or not, 0 |

#### Fetch

| Field         | Type   | Description                          |
|---------------|--------|--------------------------------------|
| Command byte  | u8     | Fixed value for the fetch command, 2 |
| partition     | u8     | Partition to fetch the message       |
| Log name size | u8     | Size of the log name in bytes        |
| Log name      | String | Log name                             |
| Group size    | u64    | Size of the group name               |
| Group         | bytes  | The group name                       |

##### Successful response

| Field        | Type   | Description                                        |
|--------------|--------|----------------------------------------------------|
| Success byte | u8     | Indicates if the response was successful or not, 0 |
| Message size | u64    | Size of the message content                        |
| Message      | String | Actual message                                     |

#### Ack

| Field         | Type   | Description                        |
|---------------|--------|------------------------------------|
| Command byte  | u8     | Fixed value for the ack command, 3 |
| partition     | u8     | Partition to fetch the message     |
| Log name size | u8     | Size of the log name in bytes      |
| Log name      | String | Log name                           |
| Group size    | u64    | Size of the group name             |
| Group         | bytes  | The group name                     |

##### Successful response

| Field        | Type   | Description                                        |
|--------------|--------|----------------------------------------------------|
| Success byte | u8     | Indicates if the response was successful or not, 0 |

#### Error response

All commands have the same possible error response.

| Field        | Type   | Description                                        |
|--------------|--------|----------------------------------------------------|
| Success byte | u8     | Indicates if the response was successful or not, 1 |
| Message size | u64    | Size of the error message                          |
| Message      | String | Detailed error message                             |

## Storage

Rog keeps the messages stored in multiple log files, each file has an
approximate max size of 1MiB. When a log is created, Rog creates a
directory with name log name and one directory for each partition, the
log files are kept inside the partition directory with the extension
`.log`.

### Log file format

The file follows a simple structure so that it is easily seekeable so
that the fetching process doesn't need to load the whole file into
memory. Each message is stored as an "entry" on the file.

| Field      | Size             | Description                            |
|------------|------------------|----------------------------------------|
| id         | 8 bytes          | The message id                         |
| entry size | 8 bytes          | The size of the entry content in bytes |
| entry      | entry size bytes | The entry content                      |

[0]: https://kafka.apache.org/
[1]: https://docs.pytest.org/en/7.4.x/
