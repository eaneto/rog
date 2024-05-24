#!/bin/sh

export ROG_HOME=~/.rog5
RUST_LOG=$1 ./target/debug/rog-server --port 7882 \
               --id 5 --node-ids 1 --node-ids 2 --node-ids 3 --node-ids 4 \
               --node-addresses "localhost:7878" \
               --node-addresses "localhost:7879" \
               --node-addresses "localhost:7880" \
               --node-addresses "localhost:7881"
