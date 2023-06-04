use atoi::atoi;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{BufMut, BytesMut};
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
    time::sleep,
};
use tracing::{debug, error, info, trace, warn};

const CREATE_LOG_COMMAND_BYTE: u8 = 48;
const PUBLISH_COMMAND_BYTE: u8 = 49;
const FETCH_COMMAND_BYTE: u8 = 50;

#[derive(Parser, Debug)]
struct Args {
    /// Port to run the server
    #[arg(short, long, default_value_t = 7878)]
    port: u16,
}

const CRLF: &str = "\r\n";

#[derive(Debug, PartialEq)]
enum Command {
    Create {
        name: String,
        partitions: usize,
    },
    Publish {
        log_name: String,
        partition: usize,
        data: BytesMut,
    },
    Fetch,
    Unknown,
}

#[derive(Debug)]
struct Message {
    partition: usize,
    data: BytesMut,
    timestamp: Instant,
}

impl Message {
    fn new(partition: usize, data: BytesMut) -> Message {
        Message {
            partition,
            data,
            timestamp: Instant::now(),
        }
    }
}

#[derive(Debug)]
struct CommitLog {
    name: String,
    senders: Vec<Sender<Message>>,
}

impl CommitLog {
    fn new(name: String, number_of_partitions: usize) -> (CommitLog, Vec<Receiver<Message>>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..number_of_partitions {
            // TODO Revisit buffer size(or make it configurable).
            let (tx, rx) = mpsc::channel(100);
            senders.push(tx);
            receivers.push(rx);
        }
        (CommitLog { name, senders }, receivers)
    }
}

/// Initialize a list a receivers to run in background tasks.
fn setup_receivers(receivers: Vec<Receiver<Message>>) {
    for mut receiver in receivers {
        tokio::spawn(async move {
            loop {
                // TODO Micro-batching
                while let Some(message) = receiver.recv().await {
                    sleep(Duration::from_millis(1000)).await;
                    debug!(
                        "Received message {:?} on partition {:?} with timestamp {:?}",
                        message.data, message.partition, message.timestamp
                    );
                }
            }
        });
    }
}

type Logs = Arc<RwLock<HashMap<String, CommitLog>>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!(port = args.port, "Running rog");

    // TODO Produce to log
    // TODO Subscribe to log
    // TODO Enable multiple subscribers to same log
    // TODO Distribute writes to multiple nodes

    let listener = match TcpListener::bind(format!("127.0.0.1:{}", args.port)).await {
        Ok(listener) => listener,
        Err(_) => panic!("Unable to start rog server on port {}", args.port),
    };

    let logs = Arc::new(RwLock::new(HashMap::<String, CommitLog>::new()));

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
            send_response(&mut stream, response).await;
            return;
        }
    };

    let response = match command {
        Command::Create { name, partitions } => {
            let (commit_log, receivers) = CommitLog::new(name.to_string(), partitions);
            setup_receivers(receivers);
            logs.write().await.insert(name.to_string(), commit_log);
            debug!(name = name, partitions = partitions, "Log created");
            format!("+OK{CRLF}")
        }
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

async fn publish_message(logs: Logs, log_name: String, partition: usize, data: BytesMut) -> String {
    match logs.read().await.get(&log_name) {
        Some(log) => {
            let message = Message::new(partition, data);
            if partition >= log.senders.len() {
                return format!(
                    "-Trying to access partition {partition} but log {log_name} has {} partitions {CRLF}",
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

fn parse_command(buf: &[u8]) -> Result<Command, String> {
    match buf[0] {
        CREATE_LOG_COMMAND_BYTE => parse_create_log_command(buf),
        PUBLISH_COMMAND_BYTE => parse_publish_log_command(buf),
        FETCH_COMMAND_BYTE => Ok(Command::Fetch),
        _ => Ok(Command::Unknown),
    }
}

fn parse_create_log_command(buf: &[u8]) -> Result<Command, String> {
    let start = 1;
    let end = buf.len() - 1;
    let index = match read_until_delimiter(buf, start, end) {
        Ok(index) => index,
        Err(index) => {
            error!(index = index, "Unparseable command");
            return Err("Unparseable command at index".to_string());
        }
    };

    let partitions = &buf[start..index];
    let partitions = match atoi::<usize>(partitions) {
        Some(partitions) => partitions,
        None => {
            error!("Invalid partition value, unable to parse to unsigned integer");
            return Err("Invalid partition value, unable to parse to unsigned integer".to_string());
        }
    };

    let start = index + 2;
    let index = match read_until_delimiter(buf, start, end) {
        Ok(index) => index,
        Err(index) => {
            error!(index = index, "Unparseable command");
            return Err("Unparseable command at index".to_string());
        }
    };

    let name = &buf[start..index];
    let name = String::from_utf8_lossy(name);

    Ok(Command::Create {
        name: name.to_string(),
        partitions,
    })
}

fn parse_publish_log_command(buf: &[u8]) -> Result<Command, String> {
    let start = 1;
    let end = buf.len() - 1;
    let index = match read_until_delimiter(buf, start, end) {
        Ok(index) => index,
        Err(index) => {
            error!(index = index, "Unparseable command");
            return Err("Unparseable command at index".to_string());
        }
    };

    let partition = &buf[start..index];
    let partition = match atoi::<usize>(partition) {
        Some(partitions) => partitions,
        None => {
            error!("Invalid partition value, unable to parse to unsigned integer");
            return Err("Invalid partition value, unable to parse to unsigned integer".to_string());
        }
    };

    let start = index + 2;
    let index = match read_until_delimiter(buf, start, end) {
        Ok(index) => index,
        Err(index) => {
            error!(index = index, "Unparseable command");
            return Err("Unparseable command at index".to_string());
        }
    };
    let log_name = &buf[start..index];
    let log_name = String::from_utf8_lossy(log_name);

    let start = index + 2;
    let index = match read_until_delimiter(buf, start, end) {
        Ok(index) => index,
        Err(index) => {
            error!(index = index, "Unparseable command");
            return Err("Unparseable command at index".to_string());
        }
    };

    let mut data = BytesMut::with_capacity(index - start);
    data.put(&buf[start..index]);

    Ok(Command::Publish {
        log_name: log_name.to_string(),
        partition,
        data,
    })
}

fn read_until_delimiter(buf: &[u8], start: usize, end: usize) -> Result<usize, i64> {
    let delimiter = CRLF.as_bytes();
    for i in start..end {
        match buf.get(i..(i + delimiter.len())) {
            Some(slice) => {
                if slice == delimiter {
                    return Ok(i);
                }
            }
            None => return Err(i as i64),
        }
    }
    Err(-1)
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_parse_create_log_command() {
        let buf = "010\r\nsome.log\r\n".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_ok());
        assert_eq!(
            command.unwrap(),
            Command::Create {
                name: "some.log".to_string(),
                partitions: 10
            }
        )
    }

    #[test]
    fn test_parse_create_log_command_with_negative_partition_number() {
        let buf = "0-1\r\nsome.log\r\n".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_err());
    }

    #[test]
    fn test_parse_create_log_command_with_invalid_partition_number() {
        let buf = "0aa\r\nsome.log\r\n".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_err());
    }

    #[test]
    fn test_parse_create_log_command_without_log_name() {
        let buf = "010\r\n".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_err());
    }

    #[test]
    fn test_parse_create_log_command_without_crlf_after_log_name() {
        let buf = "010\r\nsome.log".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_err());
    }

    #[test]
    fn test_parse_unknown_command() {
        let buf = "3example".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_ok());
        assert_eq!(command.unwrap(), Command::Unknown)
    }
}
