#!/bin/sh

export ROG_HOME=~/.rog3
RUST_LOG=$1 ./target/debug/rog-server --port 7880 \
               --id 3 --node-ids 1 --node-ids 2 \
               --node-addresses "localhost:7878" \
               --node-addresses "localhost:7879"
