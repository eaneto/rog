use atoi::atoi;
use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
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

#[derive(Debug, PartialEq)]
enum Command {
    Create {
        name: String,
        partitions: usize,
    },
    Publish {
        _log_name: String,
        _partition: usize,
        _data: Bytes,
    },
    Fetch,
    Unknown,
}

struct _Message {
    log_stream: String,
    partition: Option<usize>,
    payload: Bytes,
    timestamp: Instant,
}

#[derive(Debug)]
struct CommitLog {
    _name: String,
    _partitions: usize,
}

impl CommitLog {
    fn new(name: String, partitions: usize) -> CommitLog {
        CommitLog {
            _name: name,
            _partitions: partitions,
        }
    }
}

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

    let logs = Arc::new(Mutex::new(Vec::<CommitLog>::new()));

    handle_connections(listener, logs).await;
}

async fn handle_connections(listener: TcpListener, logs: Arc<Mutex<Vec<CommitLog>>>) {
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

async fn handle_connection(mut stream: TcpStream, logs: Arc<Mutex<Vec<CommitLog>>>) {
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
            let response = format!("ERROR: {e}\r\n");
            send_response(&mut stream, &response).await;
            return;
        }
    };

    let response = match command {
        Command::Create { name, partitions } => {
            logs.lock()
                .await
                .push(CommitLog::new(name.to_string(), partitions));
            debug!(name = name, partitions = partitions, "Log created");
            "OK\r\n"
        }
        _ => {
            debug!("command not created yet");
            "ERROR: Command not created yet"
        }
    };

    send_response(&mut stream, response).await;
}

async fn send_response(stream: &mut TcpStream, response: &str) {
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
        PUBLISH_COMMAND_BYTE => Ok(Command::Publish {
            _log_name: "example".to_string(),
            _partition: 0,
            _data: Bytes::new(),
        }),
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

fn read_until_delimiter(buf: &[u8], start: usize, end: usize) -> Result<usize, i64> {
    let delimiter = "\r\n".as_bytes();
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
