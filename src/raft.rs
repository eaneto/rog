use std::{
    cmp,
    collections::{HashMap, HashSet},
};

use tracing::{debug, error, info, trace};

use bytes::BytesMut;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};

/// The cluster must have at least 5 servers.

/// Leader Election:
/// When a server starts it starts as a follower. Leaders send
/// heartbeats([AppendEntries] with no log entries) to all followers
/// to maintain authority.

/// election timeout is a random value between 150 and 300
/// milliseconds.

/// terms

/// terms are a way to detect obsolete data.

/// Possible states:
enum State {
    Leader,
    Follower,
    Candidate,
}
/// Valid state transitions:
/// follower -> candidate
/// candidate -> leader
/// candidate -> follower
/// leader -> follower

pub struct Server {
    id: u64,
    // Need to be stored on disk
    current_term: u64,
    voted_for: Option<u64>,
    // TODO: Maybe this could be a skiplist
    log: Vec<LogEntry>,
    // Can be stored in-memory
    state: State,
    // commit_index
    commit_length: u64,
    election_timeout: u16,
    current_leader: u64,
    votes_received: HashSet<u64>,
    // sent_index
    sent_length: HashMap<u64, u64>,
    // match_index
    acked_length: HashMap<u64, u64>,
    nodes: HashMap<u64, Node>,
    last_heartbeat: Option<time::Instant>,
}

#[derive(Clone, Debug)]
pub struct Node {
    pub id: u64,
    pub address: String,
}

impl Server {
    pub fn new(id: u64, nodes: HashMap<u64, Node>) -> Server {
        Server {
            id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_length: 0,
            state: State::Follower,
            election_timeout: rand::thread_rng().gen_range(1500..3000),
            current_leader: 0,
            votes_received: HashSet::new(),
            sent_length: HashMap::new(),
            acked_length: HashMap::new(),
            nodes,
            last_heartbeat: None,
        }
    }

    pub fn election_timeout(&self) -> u16 {
        self.election_timeout
    }

    pub fn last_heartbeat(&self) -> Option<time::Instant> {
        self.last_heartbeat
    }

    pub async fn start_election(&mut self) {
        if matches!(self.state, State::Leader) {
            return;
        }

        if !matches!(self.state, State::Candidate) {
            self.current_term += 1;
            self.state = State::Candidate;
            self.voted_for = Some(self.id);
            self.votes_received.insert(self.id);
        }

        let last_term = self.last_term();

        let vote_request = VoteRequest {
            node_id: self.id,
            current_term: self.current_term,
            log_length: 0,
            last_term,
        };

        let nodes = self.nodes.clone();
        for (_, node) in &nodes {
            // TODO Send requests in parallel.
            // TODO Treat error
            trace!("Sending vote request to {}", &node.id);
            if let Ok(response) = self.send_vote_request(&vote_request, &node).await {
                self.process_vote_response(response).await;
            } else {
                break;
            }
        }

        // FIXME: If it's still a candidate, reset to follower
        // previous state.
        if matches!(self.state, State::Candidate) {
            self.current_term -= 1;
            self.state = State::Follower;
            self.voted_for = None;
            self.votes_received.remove(&self.id);
        }
    }

    async fn send_vote_request(
        &self,
        vote_request: &VoteRequest,
        node: &Node,
    ) -> Result<VoteResponse, &str> {
        let command_byte = 4_u8.to_be_bytes();
        let encoded_request = bincode::serialize(vote_request).unwrap();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(encoded_request.len().to_be_bytes());
        buf.extend(encoded_request);

        // TODO: Retry
        let mut stream = match TcpStream::connect(&node.address).await {
            Ok(stream) => stream,
            Err(_) => {
                error!("Can't connect to node at {}", &node.address);
                return Err("Can't connect to node");
            }
        };

        // What should be done in case of failure?
        match stream.write_all(&buf).await {
            Ok(()) => {
                trace!("Successfully sent request to node {}", &node.id)
            }
            Err(_) => {
                error!("Unable to send request to node {}", &node.id);
                return Err("Unable to send request to node");
            }
        };
        let mut buf = [0; 1024];
        match stream.read(&mut buf).await {
            Ok(_) => (),
            Err(_) => {
                error!("Can't read response from client {}", &node.id);
                return Err("Can't read response from client");
            }
        };
        if buf[0] == 0 {
            let length = buf.get(1..9).unwrap();
            let length = usize::from_be_bytes(length.try_into().unwrap());
            let encoded_response = match buf.get(9..(9 + length)) {
                Some(response) => response,
                None => return Err("Incomplete response, unable to parse vote response"),
            };
            let response = match bincode::deserialize(encoded_response) {
                Ok(response) => response,
                Err(_) => return Err("Unable to deserialize server response"),
            };
            Ok(response)
        } else {
            Err("Response is not successful")
        }
    }

    async fn process_vote_response(&mut self, vote_response: VoteResponse) {
        if vote_response.term > self.current_term {
            trace!("Vote response term is higher than current term, becoming follower");
            self.current_term = vote_response.term;
            self.state = State::Follower;
            self.voted_for = None;
            // TODO: Cancel election timer
        } else {
            if matches!(self.state, State::Candidate)
                && vote_response.term == self.current_term
                && vote_response.vote_in_favor
            {
                self.votes_received.insert(vote_response.node_id);
                trace!("Received vote in favor from {}", &vote_response.node_id);
                if self.votes_received.len() >= (self.nodes.len() - 1) / 2 {
                    info!("Majority of votes in favor received, becoming leader");
                    self.state = State::Leader;
                    self.current_leader = self.id;
                    // TODO: Cancel election timer
                    for (_, node) in &self.nodes {
                        self.sent_length.insert(node.id, self.log.len() as u64);
                        self.acked_length.insert(node.id, 0);
                        self.replicate_log(node).await.unwrap();
                    }
                }
            }
        }
    }

    pub async fn receive_vote(&mut self, vote_request: VoteRequest) -> VoteResponse {
        if vote_request.current_term > self.current_term {
            self.current_term = vote_request.current_term;
            self.state = State::Follower;
            self.voted_for = None;
        }
        let last_term = self.last_term();
        let ok = (vote_request.last_term > last_term)
            || (vote_request.last_term == last_term
                && vote_request.log_length >= self.log.len() as u64);
        let response = if vote_request.current_term == self.current_term
            && ok
            && (self.voted_for.is_none() || self.voted_for == Some(vote_request.node_id))
        {
            self.voted_for = Some(vote_request.node_id);
            VoteResponse {
                node_id: self.id,
                term: self.current_term,
                vote_in_favor: true,
            }
        } else {
            VoteResponse {
                node_id: self.id,
                term: self.current_term,
                vote_in_favor: false,
            }
        };

        response
    }

    fn last_term(&self) -> u64 {
        if self.log.len() > 0 {
            self.log[self.log.len() - 1].term
        } else {
            0
        }
    }

    // Log replication

    pub async fn broadcast_message(&mut self, message: BytesMut) {
        if matches!(self.state, State::Leader) {
            self.log.push(LogEntry {
                term: self.current_term,
                message,
            });
            self.acked_length.insert(self.id, self.current_term);
            self.broadcast_current_log().await;
        } else {
            unimplemented!("Leader forwarding not implemented yet")
        }
    }

    pub async fn broadcast_current_log(&mut self) {
        if matches!(self.state, State::Leader) {
            debug!("Starting log broadcast");
            for (_, node) in &self.nodes.clone() {
                if let Ok(response) = self.replicate_log(node).await {
                    self.process_log_response(response).await;
                }
            }
        }
    }

    // Can only be called by the leader
    async fn replicate_log(&self, node: &Node) -> Result<LogResponse, &str> {
        let request = match self.sent_length.get(&node.id) {
            Some(length) => {
                let prefix_length = *length as usize;
                let suffix = &self.log[prefix_length..];
                let prefix_term = if prefix_length > 0 {
                    self.log[prefix_length - 1].term
                } else {
                    0
                };
                LogRequest {
                    leader_id: self.id,
                    term: self.current_term,
                    prefix_length,
                    prefix_term,
                    leader_commit: self.commit_length,
                    suffix: suffix.to_vec(),
                }
            }
            // If there are logs sent just send an empty vector as a
            // heartbeat.
            None => {
                debug!("Sending heartbeat to replicas");
                LogRequest {
                    leader_id: self.id,
                    term: self.current_term,
                    prefix_length: 0,
                    prefix_term: 0,
                    leader_commit: self.commit_length,
                    suffix: Vec::new(),
                }
            }
        };

        self.send_log_request(&node, &request).await
    }

    // TODO: Handle errors betters, create different error types.
    async fn send_log_request(
        &self,
        node: &Node,
        log_request: &LogRequest,
    ) -> Result<LogResponse, &str> {
        let command_byte = 5_u8.to_be_bytes();
        let encoded_request = bincode::serialize(log_request).unwrap();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(encoded_request.len().to_be_bytes());
        buf.extend(encoded_request);

        // TODO: Retry
        let mut stream = match TcpStream::connect(&node.address).await {
            Ok(stream) => stream,
            Err(_) => {
                error!("Can't connect to node at {}", &node.address);
                return Err("Can't connect to node");
            }
        };

        // What should be done in case of failure?
        match stream.write_all(&buf).await {
            Ok(()) => {
                trace!("Successfully sent request to node {}", &node.id)
            }
            Err(_) => {
                error!("Unable to send request to node {}", &node.id);
                return Err("Unable to send request to node");
            }
        };
        let mut buf = [0; 1024];
        match stream.read(&mut buf).await {
            Ok(_) => (),
            Err(_) => {
                error!("Can't read response from client {}", &node.id);
                return Err("Can't read response from client");
            }
        };
        if buf[0] == 0 {
            let length = buf.get(1..9).unwrap();
            let length = usize::from_be_bytes(length.try_into().unwrap());
            let encoded_response = match buf.get(9..(9 + length)) {
                Some(response) => response,
                None => {
                    error!(
                        "Incomplete response, unable to parse log response from client {}",
                        &node.id
                    );
                    return Err("Incomplete response, unable to parse log response");
                }
            };
            let response = match bincode::deserialize(encoded_response) {
                Ok(response) => response,
                Err(_) => {
                    error!(
                        "Unable to deserialize server response from client {}",
                        &node.id
                    );
                    return Err("Unable to deserialize server response");
                }
            };
            debug!("Received successful response from {}", &node.id);
            Ok(response)
        } else {
            debug!("Received failed response from {}", &node.id);
            Err("Response is not successful")
        }
    }

    async fn process_log_response(&mut self, log_response: LogResponse) {
        if log_response.term > self.current_term {
            self.current_term = log_response.term;
            self.state = State::Follower;
            self.voted_for = None;
            // TODO: Cancel election timer
            return;
        }

        if log_response.term == self.current_term && matches!(self.state, State::Leader) {
            if log_response.successful
                && &log_response.ack >= self.acked_length.get(&log_response.node_id).unwrap()
            {
                self.sent_length
                    .insert(log_response.node_id, log_response.ack);
                self.acked_length
                    .insert(log_response.node_id, log_response.ack);
                self.commit_log_entries().await;
            } else if *self.sent_length.get(&log_response.node_id).unwrap() > 0 {
                self.sent_length.insert(
                    log_response.node_id,
                    self.sent_length.get(&log_response.node_id).unwrap() - 1,
                );
                let node = self.nodes.get(&log_response.node_id).unwrap();
                // TODO: ?
                self.replicate_log(node).await;
            }
        }
    }

    pub async fn receive_log_request(&mut self, log_request: LogRequest) -> LogResponse {
        self.last_heartbeat = Some(time::Instant::now());
        if log_request.term > self.current_term {
            self.current_term = log_request.term;
            self.voted_for = None;
            // TODO: Cancel election timer
        }
        // TODO: Is this the correct condition? Should the state be
        // modified every single time?
        if log_request.term == self.current_term {
            self.state = State::Follower;
            self.current_leader = log_request.leader_id;
        }

        let ok = (self.log.len() >= log_request.prefix_length)
            && (log_request.prefix_length == 0
                || self.log[log_request.prefix_length - 1].term == log_request.prefix_term);
        if log_request.term == self.current_term && ok {
            let ack = log_request.prefix_length + log_request.suffix.len();
            self.send_append_entries(log_request).await;
            let response = LogResponse {
                node_id: self.id,
                term: self.current_term,
                ack: ack as u64,
                successful: true,
            };
            debug!("Sending log response {:?}", response);
            response
        } else {
            let response = LogResponse {
                node_id: self.id,
                term: self.current_term,
                ack: 0,
                successful: false,
            };
            debug!("Sending log response {:?}", response);
            response
        }
    }

    async fn commit_log_entries(&mut self) {
        while self.commit_length < self.log.len() as u64 {
            let mut acks = 0;
            for (_, node) in &self.nodes {
                let acked_length = self.acked_length.get(&node.id).unwrap();
                if *acked_length > self.commit_length {
                    acks += 1;
                }
            }

            if acks >= (self.nodes.len() + 1) / 2 {
                // TODO: Deliver log
                self.commit_length += 1;
            } else {
                // No consensus
                break;
            }
        }
    }

    async fn send_append_entries(&mut self, log_request: LogRequest) {
        if log_request.suffix.len() > 0 && self.log.len() > log_request.prefix_length {
            let index = cmp::min(
                self.log.len(),
                log_request.prefix_length + log_request.suffix.len(),
            ) - 1;
            // Log is inconsistent
            if self.log[index].term != log_request.suffix[index - log_request.prefix_length].term {
                self.log = self.log[..log_request.prefix_length - 1].to_vec();
            }
        }

        if log_request.prefix_length + log_request.suffix.len() > self.log.len() {
            let start = self.log.len() - log_request.prefix_length;
            let end = log_request.suffix.len() - 1;
            for i in start..end {
                self.log.push(log_request.suffix[i].clone());
            }
        }

        if log_request.leader_commit > self.commit_length {
            for _ in (self.commit_length)..(log_request.leader_commit - 1) {
                // TODO
            }
            self.commit_length = log_request.leader_commit;
        }
    }
}

// RPC
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteRequest {
    pub node_id: u64,
    pub current_term: u64,
    pub log_length: u64,
    pub last_term: u64,
}

#[derive(Serialize, Deserialize)]
pub struct VoteResponse {
    node_id: u64,
    term: u64,
    vote_in_favor: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub message: BytesMut,
}

// RPC append entries
#[derive(Serialize, Deserialize)]
pub struct LogRequest {
    pub leader_id: u64,
    pub term: u64,
    pub prefix_length: usize,
    pub prefix_term: u64,
    pub leader_commit: u64,
    pub suffix: Vec<LogEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogResponse {
    node_id: u64,
    term: u64,
    ack: u64,
    successful: bool,
}
