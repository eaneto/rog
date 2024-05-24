use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::{BufMut, BytesMut};
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
    time::timeout,
};
use tracing::{debug, error, info, trace, warn};

use rog::{
    command::{parse_command, Command},
    log::LogSegments,
    raft::Server,
};
use rog::{
    log::{load_logs, CommitLog, InternalMessage, Logs},
    raft,
};

#[derive(Parser, Debug)]
struct Args {
    /// Port to run the server
    #[arg(short, long, default_value_t = 7878)]
    port: u16,
    /// Server's unique id
    #[arg(short, long, default_value_t = 1)]
    id: u64,
    /// List of node ids
    #[arg(long)]
    node_ids: Vec<u64>,
    /// List of node addresses
    #[arg(long)]
    node_addresses: Vec<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    info!(port = args.port, "Running rog");

    let listener = match TcpListener::bind(format!("127.0.0.1:{}", args.port)).await {
        Ok(listener) => listener,
        Err(_) => panic!("Unable to start rog server on port {}", args.port),
    };

    let logs = Arc::new(RwLock::new(HashMap::<String, CommitLog>::new()));
    let log_segments = Arc::new(RwLock::new(HashMap::new()));
    load_logs(logs.clone(), log_segments.clone()).await;

    let mut nodes = HashMap::new();
    for i in 0..(args.node_ids.len()) {
        let node_id = args.node_ids.get(i).unwrap();
        let node_address = args.node_addresses.get(i).unwrap();
        let node = raft::Node {
            id: *node_id,
            address: node_address.clone(),
        };
        nodes.insert(*node_id, node);
    }

    println!("{:?}", nodes);
    let server = Arc::new(Mutex::new(Server::new(args.id, nodes)));
    // If a new node is up it immediately tries to become the leader,
    // if there's already a leader in the cluster it will receive
    // other node's response and become a follower.
    server.lock().await.start_election().await;

    let server_clone = server.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Ok(mut server) = timeout(Duration::from_millis(200), server_clone.lock()).await {
                server.broadcast_current_log().await;
            }
        }
    });

    let election_timeout = server.lock().await.election_timeout();
    let server_clone = server.clone();
    tokio::spawn(async move {
        loop {
            let election_timeout = Duration::from_millis(election_timeout as u64);
            tokio::time::sleep(election_timeout).await;
            if let Ok(mut server) = timeout(Duration::from_millis(300), server_clone.lock()).await {
                trace!("Election timer off");
                if server.no_hearbeats_received_from_leader().await {
                    warn!("No heartbeats from leader, starting a new election");
                    server.start_election().await;
                }
            }
        }
    });

    handle_connections(server, listener, logs, log_segments).await;
}

async fn handle_connections(
    server: Arc<Mutex<raft::Server>>,
    listener: TcpListener,
    logs: Logs,
    log_segments: LogSegments,
) {
    loop {
        let server = server.clone();
        let logs = logs.clone();
        let log_segments = log_segments.clone();
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move {
                    handle_connection(server, stream, logs, log_segments).await;
                });
            }
            Err(e) => warn!("Error listening o socket {e}"),
        }
    }
}

async fn handle_connection(
    server: Arc<Mutex<raft::Server>>,
    mut stream: TcpStream,
    logs: Logs,
    log_segments: LogSegments,
) {
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

    // TODO Don't "reparse" the command here.
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
        Command::Create { name, partitions } => {
            create_log(logs, log_segments, name, partitions).await
        }
        Command::Publish {
            log_name,
            partition,
            data,
        } => publish_message(logs, log_name, partition, data).await,
        Command::Fetch {
            log_name,
            partition,
            group,
        } => fetch_log(logs, log_segments, log_name, partition, group).await,
        Command::Ack {
            partition,
            log_name,
            group,
        } => ack_message(logs, log_name, partition, group).await,
        Command::RequestVote {
            node_id,
            current_term,
            log_length,
            last_term,
        } => request_vote(server, node_id, current_term, log_length, last_term).await,
        Command::LogRequest {
            leader_id,
            term,
            prefix_length,
            prefix_term,
            leader_commit,
            suffix,
        } => {
            log_request(
                server,
                leader_id,
                term,
                prefix_length,
                prefix_term,
                leader_commit,
                suffix,
            )
            .await
        }
        Command::StartElection => {
            server.lock().await.start_election().await;

            let mut buf = Vec::new();
            buf.extend((0_u8).to_be_bytes());
            buf
        }
        _ => {
            debug!("command not created yet");
            let message = "Command not created yet";
            build_error_response(message)
        }
    };

    send_response(&mut stream, response).await;
}

async fn create_log(
    logs: Logs,
    log_segments: LogSegments,
    name: String,
    partitions: u8,
) -> Vec<u8> {
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
            CommitLog::setup_receivers(receivers, commit_log.name.clone(), &log_segments);
            let mut log_files = log_segments.write().await;
            let mut log_files_by_partition = Vec::new();
            for _ in 0..commit_log.partitions {
                log_files_by_partition.push(RwLock::new(vec![0]));
            }
            log_files.insert(name.clone(), log_files_by_partition);
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

async fn fetch_log(
    logs: Logs,
    log_segments: LogSegments,
    log_name: String,
    partition: u8,
    group: String,
) -> Vec<u8> {
    match logs.read().await.get(&log_name) {
        Some(log) => {
            let data = match log.fetch_message(log_segments, partition, group).await {
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

async fn ack_message(logs: Logs, log_name: String, partition: u8, group: String) -> Vec<u8> {
    match logs.read().await.get(&log_name) {
        Some(log) => {
            log.ack_message(partition, group).await;
            (0_u8).to_be_bytes().to_vec()
        }
        None => {
            let message = format!("No log registered with name {log_name}");
            build_error_response(&message)
        }
    }
}

async fn request_vote(
    server: Arc<Mutex<raft::Server>>,
    node_id: u64,
    current_term: u64,
    log_length: u64,
    last_term: u64,
) -> Vec<u8> {
    let vote_request = raft::VoteRequest {
        node_id,
        current_term,
        log_length,
        last_term,
    };
    debug!("Receiving vote: {:?}", vote_request);
    let mut buf = Vec::new();
    if let Ok(mut server) = timeout(Duration::from_millis(50), server.lock()).await {
        let vote_response = server.receive_vote(vote_request).await;
        let encoded_vote_response = bincode::serialize(&vote_response).unwrap();
        buf.extend((0_u8).to_be_bytes());
        buf.extend(encoded_vote_response.len().to_be_bytes());
        buf.extend(encoded_vote_response);
    } else {
        buf.extend((1_u8).to_be_bytes());
    }

    buf
}

async fn log_request(
    server: Arc<Mutex<raft::Server>>,
    leader_id: u64,
    term: u64,
    prefix_length: usize,
    prefix_term: u64,
    leader_commit: u64,
    suffix: Vec<raft::LogEntry>,
) -> Vec<u8> {
    let log_request = raft::LogRequest {
        leader_id,
        term,
        prefix_length,
        prefix_term,
        leader_commit,
        suffix,
    };

    let mut buf = Vec::new();
    if let Ok(mut server) = timeout(Duration::from_millis(50), server.lock()).await {
        let log_response = server.receive_log_request(log_request).await;
        let encoded_log_response = bincode::serialize(&log_response).unwrap();
        buf.extend((0_u8).to_be_bytes());
        buf.extend(encoded_log_response.len().to_be_bytes());
        buf.extend(encoded_log_response);
    } else {
        buf.extend((1_u8).to_be_bytes());
    }

    buf
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
