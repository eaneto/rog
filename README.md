# Rog

A simple distributed commit log inspired by [Kafka][0].

## TODOs

- Distribute writes to multiple nodes

## Building

To build both the cli and the server you can just run `cargo build
--release`.

## Integration tests

The integration tests are written in python. You can install the
dependencies with pip: `pip install -r tests/requirements.txt`. The
tests use protobuf for a few scenarios, so you need the protobuf
compiler, you can generated the necessary files running this command:

```shell
protoc -I=tests/proto --python_out=tests tests/proto/addressbook.proto
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

| Field         | type   | Description                               |
|---------------|--------|-------------------------------------------|
| Command byte  | u8     | Fixed value for the create log command, 0 |
| partitions    | u8     | Number of partitions for the log          |
| Log name size | u8     | Size of the log name in bytes             |
| Log name      | String | Log name                                  |

##### Successful response

| Field        | type | Description                                        |
|--------------|------|----------------------------------------------------|
| Success byte | u8   | Indicates if the response was successful or not, 0 |

#### Publish

| Field         | type   | Description                            |
|---------------|--------|----------------------------------------|
| Command byte  | u8     | Fixed value for the publish command, 1 |
| partition     | u8     | Partitions to publish the message      |
| Log name size | u8     | Size of the log name in bytes          |
| Log name      | String | Log name                               |
| Data size     | u64    | Size of the message in bytes           |
| Data          | bytes  | The actual content of the message      |

##### Successful response

| Field        | type | Description                                        |
|--------------|------|----------------------------------------------------|
| Success byte | u8   | Indicates if the response was successful or not, 0 |

#### Fetch

| Field         | type   | Description                                 |
|---------------|--------|---------------------------------------------|
| Command byte  | u8     | Fixed value for the fetch command, 2        |
| partition     | u8     | Partition to fetch the message              |
| Log name size | u8     | Size of the log name in bytes               |
| Log name      | String | Log name                                    |
| Data size     | u64    | Size of the content of the message in bytes |
| Data          | bytes  | The actual content of the message           |

##### Successful response

| Field        | type   | Description                                        |
|--------------|--------|----------------------------------------------------|
| Success byte | u8     | Indicates if the response was successful or not, 0 |
| Message size | u64    | Size of the message content                        |
| Message      | String | Actual message                                     |

#### Error response

All commands have the same possible error response.

| Field        | type   | Description                                        |
|--------------|--------|----------------------------------------------------|
| Success byte | u8     | Indicates if the response was successful or not, 1 |
| Message size | u64    | Size of the error message                          |
| Message      | String | Detailed error message                             |

[0]: https://kafka.apache.org/
