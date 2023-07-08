# Rog

A simple distributed commit log inspired by [Kafka][0].

## TODOs

- Distribute writes to multiple nodes

## Building

To build both the cli and the server you can just run `cargo build --release`.

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
