use std::collections::HashMap;

use bytes::BytesMut;
use rand::Rng;

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

struct Server {
    id: u64,
    current_term: u64,
    state: State,
    election_timeout: u16,
    voted_for: Option<u64>,
    next_index: HashMap<u64, u64>,
}

impl Server {
    fn _new() -> Server {
        Server {
            id: 0,
            current_term: 0,
            state: State::Follower,
            election_timeout: rand::thread_rng().gen_range(150..300),
            voted_for: None,
            next_index: HashMap::new(),
        }
    }

    // Leader election

    async fn _start_election(&mut self) {
        self.current_term += 1;
        self.state = State::Candidate;
        let _vote_request = VoteRequest {
            node_id: self.id,
            current_term: self.current_term,
            log_length: 0,
            last_term: self.current_term - 1,
        };
        // Send VoteRequest to other nodes
    }

    async fn _receive_vote(&self, vote_request: VoteRequest) {
        if vote_request.last_term >= self.current_term {
            // Ok
        }
        // Nok
    }

    // Log replication

    async fn _send_append_entries(&self) {}

    async fn _receive_append_entries(&self, _append_entries: AppendEntries) {}
}

struct VoteRequest {
    node_id: u64,
    current_term: u64,
    log_length: u64,
    last_term: u64,
}

struct LogEntry {
    term: u64,
    data: BytesMut,
}

struct AppendEntries {
    current_term: u64,
    entries: Vec<LogEntry>,
    previous_log_index: u64,
    previous_log_term: u64,
}
