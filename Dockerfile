FROM rust:1-alpine3.17

WORKDIR /usr/src/rog

COPY . .

RUN apk add --no-cache musl-dev python3 py3-pip py3-psutil

RUN rustup target add x86_64-unknown-linux-musl

RUN cargo build --release --target x86_64-unknown-linux-musl

RUN cargo install --path .

RUN python3 tests/test_runner.py
