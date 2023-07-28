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

TODO

[0]: https://kafka.apache.org/
