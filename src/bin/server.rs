use std::{collections::HashMap, sync::Arc};

use bytes::{BufMut, BytesMut};
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{debug, error, info, trace, warn};

use rog::command::{parse_command, Command};
use rog::log::{load_logs, CommitLog, InternalMessage, Logs};

#[derive(Parser, Debug)]
struct Args {
    /// Port to run the server
    #[arg(short, long, default_value_t = 7878)]
    port: u16,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!(port = args.port, "Running rog");

    // TODO Distribute writes to multiple nodes

    let listener = match TcpListener::bind(format!("127.0.0.1:{}", args.port)).await {
        Ok(listener) => listener,
        Err(_) => panic!("Unable to start rog server on port {}", args.port),
    };

    let logs = Arc::new(RwLock::new(HashMap::<String, CommitLog>::new()));
    load_logs(logs.clone()).await;

    handle_connections(listener, logs).await;
}

async fn handle_connections(listener: TcpListener, logs: Logs) {
    loop {
        let logs = logs.clone();
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_connection(stream, logs).await;
                });
            }
            Err(e) => warn!("Error listening o socket {e}"),
        }
    }
}

async fn handle_connection(mut stream: TcpStream, logs: Logs) {
    let mut cursor = 0;
    let mut buf = vec![0u8; 4096];
    loop {
        if cursor != 0 && parse_command(&buf).is_ok() {
            break;
        }

        if buf.len() == cursor {
            buf.resize(cursor * 2, 0);
        }

        let bytes_read = match stream.read(&mut buf[cursor..]).await {
            Ok(size) => size,
            Err(e) => {
                debug!("error reading tcp stream to parse command {e}");
                break;
            }
        };

        if bytes_read == 0 {
            break;
        }

        cursor += bytes_read;
    }

    let command = parse_command(&buf);

    let command = match command {
        Ok(command) => command,
        Err(e) => {
            let response = build_error_response(e);
            send_response(&mut stream, response).await;
            return;
        }
    };

    let response = match command {
        Command::Create { name, partitions } => create_log(logs, name, partitions).await,
        Command::Publish {
            log_name,
            partition,
            data,
        } => publish_message(logs, log_name, partition, data).await,
        Command::Fetch {
            log_name,
            partition,
            group,
        } => fetch_log(logs, log_name, partition, group).await,
        _ => {
            debug!("command not created yet");
            let message = "Command not created yet";
            build_error_response(message)
        }
    };

    send_response(&mut stream, response).await;
}

async fn create_log(logs: Logs, name: String, partitions: u8) -> Vec<u8> {
    if partitions == 0 {
        let message = "Number of partitions must be at least 1";
        return build_error_response(message);
    }

    if logs.read().await.contains_key(&name) {
        let message = format!("Log {name} already exists");
        return build_error_response(&message);
    }

    let (commit_log, receivers) = CommitLog::new(name.to_string(), partitions);
    match commit_log.create_log_files().await {
        Ok(()) => {
            CommitLog::setup_receivers(receivers, commit_log.name.clone());
            logs.write().await.insert(name.to_string(), commit_log);
            debug!(name = name, partitions = partitions, "Log created");
            0_u8.to_be_bytes().to_vec()
        }
        Err(e) => build_error_response(e),
    }
}

async fn publish_message(logs: Logs, log_name: String, partition: u8, data: BytesMut) -> Vec<u8> {
    match logs.read().await.get(&log_name) {
        Some(log) => {
            let message = InternalMessage::new(partition, data);
            if partition >= log.partitions {
                let message = format!(
                    "Trying to access partition {partition} but log {log_name} has {} partitions",
                    log.partitions
                );
                return build_error_response(&message);
            }
            match log.send_message(message).await {
                Ok(_) => {
                    debug!(
                        log_name = log_name,
                        partition = partition,
                        "Successfully published message"
                    );
                    (0_u8).to_be_bytes().to_vec()
                }
                Err(e) => {
                    error!(
                        log_name = log_name,
                        partition = partition,
                        "Unable to publish message {:?}",
                        e
                    );
                    let message = "Unable to send message to channel";
                    build_error_response(message)
                }
            }
        }
        None => {
            let message = format!("No log registered with name {log_name}");
            build_error_response(&message)
        }
    }
}

async fn fetch_log(logs: Logs, log_name: String, partition: u8, group: String) -> Vec<u8> {
    match logs.read().await.get(&log_name) {
        Some(log) => {
            let data = match log.fetch_message(partition, group).await {
                Ok(data) => {
                    let mut response = BytesMut::with_capacity(1 + 8 + data.capacity());
                    response.put_u8(0);
                    response.put_u64(data.capacity() as u64);
                    response.put(data);
                    response
                }
                Err(e) => {
                    let message_bytes = e.as_bytes();
                    let mut data = BytesMut::with_capacity(1 + 8 + message_bytes.len());
                    data.put_u8(1);
                    data.put_u64(message_bytes.len() as u64);
                    data.put(message_bytes);
                    data
                }
            };
            data.to_vec()
        }
        None => {
            let message = format!("No log registered with name {log_name}");
            build_error_response(&message)
        }
    }
}

/// Builds the standard error response, the first byte is always an u8
/// 1 to represent the error, the next 8 bytes are the message size
/// followed by the actual error message.
fn build_error_response(message: &str) -> Vec<u8> {
    let message = message.as_bytes();
    let mut response = Vec::new();
    response.extend((1_u8).to_be_bytes());
    response.extend(message.len().to_be_bytes());
    response.extend(message);
    response
}

async fn send_response(stream: &mut TcpStream, response: Vec<u8>) {
    match stream.write_all(&response).await {
        Ok(()) => {
            trace!("Successfully sent response to client")
        }
        Err(e) => {
            error!("Unable to send response to client {e}");
        }
    };
}
