#!/bin/sh

export ROG_HOME=~/.rog2
RUST_LOG=$1 ./target/debug/rog-server --port 7879 \
               --id 2 --node-ids 1 --node-ids 3 --node-ids 4 --node-ids 5 \
               --node-addresses "localhost:7878" \
               --node-addresses "localhost:7880" \
               --node-addresses "localhost:7881" \
               --node-addresses "localhost:7882"
