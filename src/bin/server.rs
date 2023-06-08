use std::{collections::HashMap, env, sync::Arc};

use bytes::BytesMut;
use clap::Parser;
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{debug, error, info, trace, warn};

use rog::command::{parse_command, Command, CRLF};
use rog::log::{setup_receivers, CommitLog, InternalMessage, Logs};

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

    // TODO Subscribe to log
    // TODO Enable multiple subscribers to same log
    // TODO Distribute writes to multiple nodes

    let listener = match TcpListener::bind(format!("127.0.0.1:{}", args.port)).await {
        Ok(listener) => listener,
        Err(_) => panic!("Unable to start rog server on port {}", args.port),
    };

    let logs = Arc::new(RwLock::new(HashMap::<String, CommitLog>::new()));
    load_logs(logs.clone()).await;

    handle_connections(listener, logs).await;
}

async fn load_logs(logs: Logs) {
    let rog_home = match env::var("ROG_HOME") {
        Ok(path) => path,
        Err(e) => panic!("ROG_HOME enrivonment variable not set {e}"),
    };

    let mut dir = match fs::read_dir(&rog_home).await {
        Ok(dir) => dir,
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => return,
            _ => panic!("Unable to read {rog_home}\n{e}"),
        },
    };

    while let Some(entry) = dir.next_entry().await.unwrap() {
        let file_name = entry.file_name();
        let log_name = file_name.to_string_lossy();
        let partitions = std::fs::read_dir(format!("{rog_home}/{log_name}"))
            .unwrap()
            .count();
        let (commit_log, receivers) = CommitLog::new(log_name.to_string(), partitions);
        setup_receivers(receivers);
        logs.write().await.insert(log_name.to_string(), commit_log);
    }
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
        _ => {
            debug!("command not created yet");
            format!("-Command not created yet{CRLF}")
        }
    };

    send_response(&mut stream, response).await;
}

async fn create_log(logs: Logs, name: String, partitions: usize) -> String {
    if logs.read().await.contains_key(&name) {
        return format!("-Log {name} already exists{CRLF}");
    }

    let (commit_log, receivers) = CommitLog::new(name.to_string(), partitions);
    match commit_log.create_log_files().await {
        Ok(()) => {
            setup_receivers(receivers);
            logs.write().await.insert(name.to_string(), commit_log);
            debug!(name = name, partitions = partitions, "Log created");
            format!("+OK{CRLF}")
        }
        Err(e) => {
            format!("-{e}{CRLF}")
        }
    }
}

async fn publish_message(logs: Logs, log_name: String, partition: usize, data: BytesMut) -> String {
    match logs.read().await.get(&log_name) {
        Some(log) => {
            let message = InternalMessage::new(log.name.clone(), partition, data);
            if partition >= log.senders.len() {
                return format!(
                    "-Trying to access partition {partition} but log {log_name} has {} partitions{CRLF}",
                    log.senders.len()
                );
            }
            match log.senders[partition].send(message).await {
                Ok(_) => {
                    debug!(
                        log_name = log_name,
                        partition = partition,
                        "Successfully published message"
                    );
                    format!("+OK{CRLF}")
                }
                Err(e) => {
                    error!(
                        log_name = log_name,
                        partition = partition,
                        "Unable to publish message {:?}",
                        e
                    );
                    format!("-Unable to send message to channel{CRLF}")
                }
            }
        }
        None => format!("-No log registered with name {log_name}{CRLF}"),
    }
}

async fn send_response(stream: &mut TcpStream, response: String) {
    match stream.write_all(response.as_bytes()).await {
        Ok(()) => {
            trace!("Successfully sent response to client")
        }
        Err(e) => {
            error!("Unable to send response to client {e}");
        }
    };
}
