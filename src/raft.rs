use std::{cmp, collections::HashMap, sync::atomic::AtomicU64, time::Duration};

use crossbeam_skiplist::{SkipMap, SkipSet};
use tracing::{debug, error, info, trace};

use bytes::BytesMut;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::RwLock,
    time::{self, Instant},
};

enum State {
    Leader,
    Follower,
    Candidate,
}

pub struct Server {
    id: NodeId,
    // Need to be stored on disk
    current_term: AtomicU64,
    voted_for: RwLock<Option<NodeId>>,
    log: RwLock<Vec<LogEntry>>,
    // Can be stored in-memory
    state: RwLock<State>,
    // commit_index
    commit_length: AtomicU64,
    election_timeout: u16,
    current_leader: AtomicU64,
    votes_received: SkipSet<NodeId>,
    // sent_index
    sent_length: SkipMap<NodeId, u64>,
    // match_index
    acked_length: SkipMap<NodeId, u64>,
    nodes: RwLock<HashMap<NodeId, Node>>,
    last_heartbeat: RwLock<Option<time::Instant>>,
}

type NodeId = u64;

#[derive(Clone, Debug)]
pub struct Node {
    pub id: NodeId,
    pub address: String,
}

impl Server {
    pub fn new(id: u64, nodes: HashMap<u64, Node>) -> Server {
        Server {
            id,
            current_term: AtomicU64::new(0),
            voted_for: RwLock::new(None),
            log: RwLock::new(Vec::new()),
            state: RwLock::new(State::Follower),
            commit_length: AtomicU64::new(0),
            election_timeout: rand::thread_rng().gen_range(150..300),
            current_leader: AtomicU64::new(0),
            votes_received: SkipSet::new(),
            sent_length: SkipMap::new(),
            acked_length: SkipMap::new(),
            nodes: RwLock::new(nodes.clone()),
            last_heartbeat: RwLock::new(None),
        }
    }

    fn current_term(&self) -> u64 {
        self.current_term.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn increment_current_term(&self) {
        self.current_term
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn decrement_current_term(&self) {
        self.current_term
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn update_current_term(&self, value: u64) {
        self.current_term
            .store(value, std::sync::atomic::Ordering::SeqCst)
    }

    async fn is_candidate(&self) -> bool {
        matches!(*self.state.read().await, State::Candidate)
    }

    pub async fn is_leader(&self) -> bool {
        matches!(*self.state.read().await, State::Leader)
    }

    async fn become_leader(&mut self) {
        let mut state = self.state.write().await;
        *state = State::Leader;
    }

    async fn become_follower(&mut self) {
        let mut state = self.state.write().await;
        *state = State::Follower;
    }

    async fn become_candidate(&mut self) {
        let mut state = self.state.write().await;
        *state = State::Candidate;
    }

    fn commit_length(&self) -> u64 {
        self.commit_length.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn increment_commit_length(&self) {
        self.commit_length
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn update_commit_length(&self, value: u64) {
        self.commit_length
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn election_timeout(&self) -> u16 {
        self.election_timeout
    }

    fn vote_on_self(&self) {
        self.votes_received.insert(self.id);
    }

    async fn unvote(&self) {
        let mut voted_for = self.voted_for.write().await;
        *voted_for = None;
        self.votes_received.clear();
    }

    fn vote_on_new_leader(&self, node: u64) {
        self.votes_received.insert(node);
    }

    fn update_sent_length(&self, node_id: NodeId, length: u64) {
        self.sent_length.insert(node_id, length);
    }

    fn decrement_sent_length(&self, node_id: NodeId) {
        let sent_length_by_node = self.sent_length.get(&node_id).unwrap();
        self.sent_length
            .insert(node_id, sent_length_by_node.value() - 1);
    }

    pub async fn last_heartbeat(&self) -> Option<time::Instant> {
        *self.last_heartbeat.read().await
    }

    async fn update_last_heartbeat(&mut self) {
        let mut last_heartbeat = self.last_heartbeat.write().await;
        *last_heartbeat = Some(time::Instant::now());
    }

    async fn log_length(&self) -> usize {
        self.log.read().await.len()
    }

    /// Checks if the last heartbeat received from the leader(if there
    /// is a last heartbeat) has passed the election timeout.
    pub async fn no_hearbeats_received_from_leader(&self) -> bool {
        let election_timeout = Duration::from_millis(self.election_timeout() as u64);
        let time_elapsed = Instant::now() - election_timeout;
        let last_heartbeat = self.last_heartbeat().await;
        (last_heartbeat
            .is_some_and(|heartbeat| time_elapsed.duration_since(heartbeat) > election_timeout)
            || last_heartbeat.is_none())
            && !self.is_leader().await
    }

    pub async fn start_election(&mut self) {
        if self.is_leader().await {
            return;
        }

        if !self.is_candidate().await {
            self.increment_current_term();
            self.become_candidate().await;
            let mut voted_for = self.voted_for.write().await;
            *voted_for = Some(self.id);
            self.vote_on_self();
        }

        let last_term = self.last_term().await;

        let vote_request = VoteRequest {
            node_id: self.id,
            current_term: self.current_term(),
            log_length: 0,
            last_term,
        };

        let nodes = self.nodes.read().await.clone();
        for node in nodes.values() {
            // TODO Send requests in parallel.
            // TODO Treat error
            trace!("Sending vote request to {}", &node.id);
            if let Ok(response) = self.send_vote_request(&vote_request, node).await {
                self.process_vote_response(response).await;
            }
        }

        if self.is_candidate().await {
            self.decrement_current_term();
            self.become_follower().await;
            self.unvote().await;
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
        if vote_response.term > self.current_term() {
            trace!("Vote response term is higher than current term, becoming follower");
            self.update_current_term(vote_response.term);
            self.become_follower().await;
            self.unvote().await;
            // TODO: Cancel election timer
        } else if self.is_candidate().await
            && vote_response.term == self.current_term()
            && vote_response.vote_in_favor
        {
            self.vote_on_new_leader(vote_response.node_id);
            trace!("Received vote in favor from {}", &vote_response.node_id);
            if self.has_majority_of_votes().await {
                info!("Majority of votes in favor received, becoming leader");
                self.become_leader().await;
                self.current_leader
                    .store(self.id, std::sync::atomic::Ordering::SeqCst);
                // TODO: Cancel election timer
                let nodes = self.nodes.read().await.clone();
                for node in nodes.values() {
                    self.update_sent_length(node.id, self.log.read().await.len() as u64);
                    self.acked_length.insert(node.id, 0);
                    // FIXME
                    let _ = self.replicate_log(node).await;
                }
            }
        }
    }

    async fn has_majority_of_votes(&self) -> bool {
        // Each server has a list of other nodes in the cluster, so
        // the number of nodes is the cluster is the current number of
        // nodes + 1(itself).
        let nodes_on_cluster = self.nodes.read().await.len() + 1;
        // Increments the number of nodes on cluster for dividing up,
        // e.g. if there are 5 nodes, the majority must be 3, without
        // the increment diving five by two will result in 2 instead
        // of 3.
        let majority = (nodes_on_cluster + 1) / 2;
        let votes_received = self.votes_received.len();
        votes_received >= majority
    }

    pub async fn receive_vote(&mut self, vote_request: VoteRequest) -> VoteResponse {
        if vote_request.current_term > self.current_term() {
            self.update_current_term(vote_request.current_term);
            self.become_follower().await;
            self.unvote().await;
        }
        let last_term = self.last_term().await;
        let ok = (vote_request.last_term > last_term)
            || (vote_request.last_term == last_term
                && vote_request.log_length >= self.log.read().await.len() as u64);
        let mut voted_for = self.voted_for.write().await;
        if vote_request.current_term == self.current_term()
            && ok
            && (voted_for.is_none() || *voted_for == Some(vote_request.node_id))
        {
            *voted_for = Some(vote_request.node_id);
            VoteResponse {
                node_id: self.id,
                term: self.current_term(),
                vote_in_favor: true,
            }
        } else {
            VoteResponse {
                node_id: self.id,
                term: self.current_term(),
                vote_in_favor: false,
            }
        }
    }

    async fn last_term(&self) -> u64 {
        let log = self.log.read().await;
        if log.len() > 0 {
            log[log.len() - 1].term
        } else {
            0
        }
    }

    // Log replication

    pub async fn broadcast_message(&mut self, message: BytesMut) {
        if self.is_leader().await {
            self.log.write().await.push(LogEntry {
                term: self.current_term(),
                message,
            });
            self.acked_length.insert(self.id, self.current_term());
            self.broadcast_current_log().await;
        } else {
            unimplemented!("Leader forwarding not implemented yet")
        }
    }

    pub async fn broadcast_current_log(&mut self) {
        if self.is_leader().await {
            trace!("Starting log broadcast");
            let nodes = self.nodes.read().await.clone();
            for node in nodes.values() {
                if let Ok(response) = self.replicate_log(node).await {
                    self.process_log_response(response).await;
                }
            }
        }
    }

    // Can only be called by the leader
    async fn replicate_log(&self, node: &Node) -> Result<LogResponse, &str> {
        let request = match self.sent_length.get(&node.id) {
            Some(entry) => {
                let prefix_length = *entry.value() as usize;
                let suffix = &self.log.read().await[prefix_length..];
                let prefix_term = if prefix_length > 0 {
                    self.log.read().await[prefix_length - 1].term
                } else {
                    0
                };
                LogRequest {
                    leader_id: self.id,
                    term: self.current_term(),
                    prefix_length,
                    prefix_term,
                    leader_commit: self.commit_length(),
                    suffix: suffix.to_vec(),
                }
            }
            // If there are logs sent just send an empty vector as a
            // heartbeat.
            None => {
                debug!("Sending heartbeat to replicas");
                LogRequest {
                    leader_id: self.id,
                    term: self.current_term(),
                    prefix_length: 0,
                    prefix_term: 0,
                    leader_commit: self.commit_length(),
                    suffix: Vec::new(),
                }
            }
        };

        self.send_log_request(node, &request).await
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
            trace!("Received successful response from {}", &node.id);
            Ok(response)
        } else {
            trace!("Received failed response from {}", &node.id);
            Err("Response is not successful")
        }
    }

    async fn process_log_response(&mut self, log_response: LogResponse) {
        if log_response.term > self.current_term() {
            self.update_current_term(log_response.term);
            self.become_follower().await;
            self.unvote().await;
            // TODO: Cancel election timer
            return;
        }

        if log_response.term == self.current_term() && self.is_leader().await {
            if log_response.successful
                && &log_response.ack
                    >= self
                        .acked_length
                        .get(&log_response.node_id)
                        .unwrap()
                        .value()
            {
                self.update_sent_length(log_response.node_id, log_response.ack);
                self.acked_length
                    .insert(log_response.node_id, log_response.ack);
                self.commit_log_entries().await;
            } else if *self.sent_length.get(&log_response.node_id).unwrap().value() > 0 {
                self.decrement_sent_length(log_response.node_id);

                let nodes = self.nodes.read().await;
                let node = nodes.get(&log_response.node_id).unwrap();
                // TODO: ?
                let _ = self.replicate_log(node).await;
            }
        }
    }

    pub async fn receive_log_request(&mut self, log_request: LogRequest) -> LogResponse {
        self.update_last_heartbeat().await;
        if log_request.term > self.current_term() {
            self.update_current_term(log_request.term);
            self.unvote().await;
            // TODO: Cancel election timer
        }
        // TODO: Is this the correct condition? Should the state be
        // modified every single time?
        if log_request.term == self.current_term() {
            self.become_follower().await;
            self.current_leader
                .store(log_request.leader_id, std::sync::atomic::Ordering::SeqCst);
        }

        let ok = (self.log_length().await >= log_request.prefix_length)
            && (log_request.prefix_length == 0
                || self.log.read().await[log_request.prefix_length - 1].term
                    == log_request.prefix_term);
        if log_request.term == self.current_term() && ok {
            let ack = log_request.prefix_length + log_request.suffix.len();
            self.send_append_entries(log_request).await;
            let response = LogResponse {
                node_id: self.id,
                term: self.current_term(),
                ack: ack as u64,
                successful: true,
            };
            trace!("Sending log response {:?}", response);
            response
        } else {
            let response = LogResponse {
                node_id: self.id,
                term: self.current_term(),
                ack: 0,
                successful: false,
            };
            trace!("Sending log response {:?}", response);
            response
        }
    }

    async fn commit_log_entries(&mut self) {
        while self.commit_length() < self.log_length().await as u64 {
            let mut acks = 0;
            let nodes = self.nodes.read().await.clone();
            for node in nodes.values() {
                let acked_length = *self.acked_length.get(&node.id).unwrap().value();
                if acked_length > self.commit_length() {
                    acks += 1;
                }
            }

            if acks >= (self.nodes.read().await.len() + 1) / 2 {
                // TODO: Deliver log
                self.increment_commit_length();
            } else {
                // No consensus
                break;
            }
        }
    }

    async fn send_append_entries(&mut self, log_request: LogRequest) {
        let mut log = self.log.write().await;
        let log_length = log.len();
        if !log_request.suffix.is_empty() && log_length > log_request.prefix_length {
            let index = cmp::min(
                log_length,
                log_request.prefix_length + log_request.suffix.len(),
            ) - 1;
            // Log is inconsistent
            if log[index].term != log_request.suffix[index - log_request.prefix_length].term {
                *log = log[..log_request.prefix_length - 1].to_vec();
            }
        }

        if log_request.prefix_length + log_request.suffix.len() > log_length {
            let start = log_length - log_request.prefix_length;
            let end = log_request.suffix.len() - 1;
            for i in start..end {
                log.push(log_request.suffix[i].clone());
            }
        }

        if log_request.leader_commit > self.commit_length() {
            for _ in (self.commit_length())..(log_request.leader_commit - 1) {
                // TODO
            }
            self.update_commit_length(log_request.leader_commit);
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

#[cfg(test)]
mod tests {

    use crate::raft::*;

    #[tokio::test]
    async fn should_start_a_new_election_with_no_hearbeats() {
        let server = Server::new(1, HashMap::new());

        let result = server.no_hearbeats_received_from_leader().await;

        assert!(result);
    }

    #[tokio::test]
    async fn should_start_a_new_election_with_outdated_hearbeats() {
        let one_second_ago = Instant::now() - Duration::from_secs(1);
        let server = Server {
            id: 1,
            current_term: AtomicU64::new(0),
            voted_for: RwLock::new(None),
            log: RwLock::new(Vec::new()),
            state: RwLock::new(State::Follower),
            commit_length: AtomicU64::new(0),
            election_timeout: rand::thread_rng().gen_range(150..300),
            current_leader: AtomicU64::new(0),
            votes_received: SkipSet::new(),
            sent_length: SkipMap::new(),
            acked_length: SkipMap::new(),
            nodes: RwLock::new(HashMap::new()),
            last_heartbeat: RwLock::new(Option::Some(one_second_ago)),
        };

        let result = server.no_hearbeats_received_from_leader().await;

        assert!(result);
    }

    #[tokio::test]
    async fn should_not_start_a_new_election() {
        let server = Server {
            id: 1,
            current_term: AtomicU64::new(0),
            voted_for: RwLock::new(None),
            log: RwLock::new(Vec::new()),
            state: RwLock::new(State::Follower),
            commit_length: AtomicU64::new(0),
            election_timeout: rand::thread_rng().gen_range(150..300),
            current_leader: AtomicU64::new(0),
            votes_received: SkipSet::new(),
            sent_length: SkipMap::new(),
            acked_length: SkipMap::new(),
            nodes: RwLock::new(HashMap::new()),
            last_heartbeat: RwLock::new(Option::Some(Instant::now())),
        };

        let result = server.no_hearbeats_received_from_leader().await;

        assert!(!result);
    }
}
