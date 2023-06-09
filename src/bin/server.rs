use std::{collections::HashMap, sync::Arc};

use bytes::{BufMut, BytesMut};
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{debug, error, info, trace, warn};

use rog::command::{parse_command, Command, CRLF};
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
    // TODO Fix for packets over 4kb
    loop {
        let bytes_read = match stream.read(&mut buf[cursor..]).await {
            Ok(size) => size,
            Err(_) => break,
        };

        if bytes_read == 0 {
            break;
        }

        if buf.len() == cursor {
            buf.resize(cursor * 2, 0);
        }

        if bytes_read < buf.len() {
            break;
        } else {
            cursor += bytes_read;
        }
    }

    let command = parse_command(&buf);

    let command = match command {
        Ok(command) => command,
        Err(e) => {
            let response = format!("-{e}{CRLF}");
            send_response(&mut stream, response.as_bytes().to_vec()).await;
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
            let response = format!("-Command not created yet{CRLF}");
            response.as_bytes().to_vec()
        }
    };

    send_response(&mut stream, response).await;
}

async fn create_log(logs: Logs, name: String, partitions: usize) -> Vec<u8> {
    if partitions == 0 {
        let response = format!("-Number of partitions must be at least 1{CRLF}");
        return response.as_bytes().to_vec();
    }

    if logs.read().await.contains_key(&name) {
        let response = format!("-Log {name} already exists{CRLF}");
        return response.as_bytes().to_vec();
    }

    let (commit_log, receivers) = CommitLog::new(name.to_string(), partitions);
    match commit_log.create_log_files().await {
        Ok(()) => {
            CommitLog::setup_receivers(receivers, commit_log.name.clone());
            logs.write().await.insert(name.to_string(), commit_log);
            debug!(name = name, partitions = partitions, "Log created");
            let response = format!("+OK{CRLF}");
            response.as_bytes().to_vec()
        }
        Err(e) => {
            let response = format!("-{e}{CRLF}");
            response.as_bytes().to_vec()
        }
    }
}

async fn publish_message(
    logs: Logs,
    log_name: String,
    partition: usize,
    data: BytesMut,
) -> Vec<u8> {
    match logs.read().await.get(&log_name) {
        Some(log) => {
            let message = InternalMessage::new(partition, data);
            if partition >= log.partitions {
                let response = format!(
                    "-Trying to access partition {partition} but log {log_name} has {} partitions{CRLF}",
                    log.partitions
                );
                return response.as_bytes().to_vec();
            }
            match log.send_message(message).await {
                Ok(_) => {
                    debug!(
                        log_name = log_name,
                        partition = partition,
                        "Successfully published message"
                    );
                    let response = format!("+OK{CRLF}");
                    response.as_bytes().to_vec()
                }
                Err(e) => {
                    error!(
                        log_name = log_name,
                        partition = partition,
                        "Unable to publish message {:?}",
                        e
                    );
                    let response = format!("-Unable to send message to channel{CRLF}");
                    response.as_bytes().to_vec()
                }
            }
        }
        None => {
            let response = format!("-No log registered with name {log_name}{CRLF}");
            response.as_bytes().to_vec()
        }
    }
}

async fn fetch_log(logs: Logs, log_name: String, partition: usize, group: String) -> Vec<u8> {
    match logs.read().await.get(&log_name) {
        Some(log) => {
            let data = match log.fetch_message(partition, group).await {
                Ok(data) => data,
                Err(e) => {
                    let mut data = BytesMut::with_capacity(e.len());
                    data.put(e.as_bytes());
                    data
                }
            };
            data.to_vec()
        }
        None => {
            let response = format!("-No log registered with name {log_name}{CRLF}");
            response.as_bytes().to_vec()
        }
    }
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
