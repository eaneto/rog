#!/bin/sh

export ROG_HOME=~/.rog1
RUST_LOG=trace ./target/debug/rog-server --port 7878 \
               --id 1 --node-ids 2 --node-ids 3 \
               --node-addresses "localhost:7879" \
               --node-addresses "localhost:7880"
