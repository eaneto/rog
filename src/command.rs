use bytes::{BufMut, BytesMut};

use crate::raft;
use bincode;

const CREATE_LOG_COMMAND_BYTE: u8 = 0;
const PUBLISH_COMMAND_BYTE: u8 = 1;
const FETCH_COMMAND_BYTE: u8 = 2;
const ACK_COMMAND_BYTE: u8 = 3;
const REQUEST_VOTE_COMMAND_BYTE: u8 = 4;
const LOG_REQUEST_COMMAND_BYTE: u8 = 5;

#[derive(Debug, PartialEq)]
pub enum Command {
    Create {
        name: String,
        partitions: u8,
    },
    Publish {
        partition: u8,
        log_name: String,
        data: BytesMut,
    },
    Fetch {
        partition: u8,
        log_name: String,
        group: String,
    },
    Ack {
        partition: u8,
        log_name: String,
        group: String,
    },
    RequestVote {
        node_id: u64,
        current_term: u64,
        log_length: u64,
        last_term: u64,
    },
    LogRequest {
        leader_id: u64,
        term: u64,
        prefix_length: usize,
        prefix_term: u64,
        leader_commit: u64,
        suffix: Vec<raft::LogEntry>,
    },
    Unknown,
}

pub fn parse_command(buf: &[u8]) -> Result<Command, &str> {
    let command_byte = match buf.first() {
        Some(command_byte) => *command_byte,
        None => return Err("Unable to parse command byte"),
    };

    match command_byte {
        CREATE_LOG_COMMAND_BYTE => parse_create_log_command(buf),
        PUBLISH_COMMAND_BYTE => parse_publish_log_command(buf),
        FETCH_COMMAND_BYTE => parse_fetch_log_command(buf),
        ACK_COMMAND_BYTE => parse_ack_command(buf),
        REQUEST_VOTE_COMMAND_BYTE => parse_request_vote_command(buf),
        LOG_REQUEST_COMMAND_BYTE => parse_log_request_command(buf),
        _ => Ok(Command::Unknown),
    }
}

fn parse_create_log_command(buf: &[u8]) -> Result<Command, &str> {
    let partitions = match buf.get(1) {
        Some(partitions) => *partitions,
        None => {
            return Err("Unparseable command, unable to parse partitions");
        }
    };
    let name_size = match buf.get(2) {
        Some(name_size) => *name_size,
        None => {
            return Err("Unparseable command, unable to parse name size");
        }
    };

    let name = match buf.get(3..(3 + name_size as usize)) {
        Some(name) => name,
        None => {
            return Err("Unparseable command, unable to parse name");
        }
    };
    let name = String::from_utf8_lossy(name);

    Ok(Command::Create {
        name: name.to_string(),
        partitions,
    })
}

fn parse_publish_log_command(buf: &[u8]) -> Result<Command, &str> {
    let partition = match buf.get(1) {
        Some(partition) => *partition,
        None => {
            return Err("Unparseable command, unable to parse partition");
        }
    };
    let name_size = match buf.get(2) {
        Some(name_size) => *name_size,
        None => {
            return Err("Unparseable command, unable to parse name size");
        }
    };

    let log_name = match buf.get(3..(3 + name_size as usize)) {
        Some(log_name) => log_name,
        None => {
            return Err("Unparseable command, unable to parse name ");
        }
    };
    let log_name = String::from_utf8_lossy(log_name);

    let data_size = match buf.get((3 + name_size as usize)..(3 + name_size as usize + 8)) {
        Some(data_size) => data_size,
        None => {
            return Err("Unparseable command, unable to parse data size");
        }
    };
    let data_size = usize::from_be_bytes(data_size.try_into().unwrap());
    let mut data = BytesMut::with_capacity(data_size);
    match buf.get((3 + 8 + name_size as usize)..(3 + 8 + name_size as usize + data_size)) {
        Some(content) => data.put(content),
        None => {
            return Err("Unparseable command, unable to parse data");
        }
    }

    Ok(Command::Publish {
        log_name: log_name.to_string(),
        partition,
        data,
    })
}

fn parse_fetch_log_command(buf: &[u8]) -> Result<Command, &str> {
    let partition = match buf.get(1) {
        Some(partition) => *partition,
        None => {
            return Err("Unparseable command, unable to parse partition");
        }
    };
    let name_size = match buf.get(2) {
        Some(name_size) => *name_size,
        None => {
            return Err("Unparseable command, unable to parse name size");
        }
    };

    let log_name = match buf.get(3..(3 + name_size as usize)) {
        Some(log_name) => log_name,
        None => {
            return Err("Unparseable command, unable to parse name");
        }
    };
    let log_name = String::from_utf8_lossy(log_name);

    let group_size = match buf.get(3 + name_size as usize) {
        Some(group_size) => *group_size,
        None => {
            return Err("Unparseable command, unable to parse group size");
        }
    };
    let group =
        match buf.get((4 + name_size as usize)..(4 + name_size as usize + group_size as usize)) {
            Some(group) => group,
            None => {
                return Err("Unparseable command, unable to parse group");
            }
        };
    let group = String::from_utf8_lossy(group);

    Ok(Command::Fetch {
        log_name: log_name.to_string(),
        partition,
        group: group.to_string(),
    })
}

fn parse_ack_command(buf: &[u8]) -> Result<Command, &str> {
    let partition = match buf.get(1) {
        Some(partition) => *partition,
        None => {
            return Err("Unparseable command, unable to parse partition");
        }
    };
    let name_size = match buf.get(2) {
        Some(name_size) => *name_size,
        None => {
            return Err("Unparseable command, unable to parse name size");
        }
    };

    let log_name = match buf.get(3..(3 + name_size as usize)) {
        Some(log_name) => log_name,
        None => {
            return Err("Unparseable command, unable to parse name");
        }
    };
    let log_name = String::from_utf8_lossy(log_name);

    let group_size = match buf.get(3 + name_size as usize) {
        Some(group_size) => *group_size,
        None => {
            return Err("Unparseable command, unable to parse group size");
        }
    };
    let group =
        match buf.get((4 + name_size as usize)..(4 + name_size as usize + group_size as usize)) {
            Some(group) => group,
            None => {
                return Err("Unparseable command, unable to parse group");
            }
        };
    let group = String::from_utf8_lossy(group);

    Ok(Command::Ack {
        log_name: log_name.to_string(),
        partition,
        group: group.to_string(),
    })
}

fn parse_request_vote_command(buf: &[u8]) -> Result<Command, &str> {
    let length = match buf.get(1..9) {
        Some(length) => length,
        None => {
            return Err("Unparseable command, unable to parse length");
        }
    };

    let length = usize::from_be_bytes(length.try_into().unwrap());
    let vote_request_buffer = buf.get(9..(9 + length)).unwrap();
    let vote_request: raft::VoteRequest = bincode::deserialize(vote_request_buffer).unwrap();

    return Ok(Command::RequestVote {
        node_id: vote_request.node_id,
        current_term: vote_request.current_term,
        log_length: vote_request.log_length,
        last_term: vote_request.last_term,
    });
}

fn parse_log_request_command(buf: &[u8]) -> Result<Command, &str> {
    let length = match buf.get(1..9) {
        Some(length) => length,
        None => {
            return Err("Unparseable command, unable to parse length");
        }
    };

    let length = usize::from_be_bytes(length.try_into().unwrap());
    let log_request_buffer = buf.get(9..(9 + length)).unwrap();
    let log_request: raft::LogRequest = bincode::deserialize(log_request_buffer).unwrap();

    return Ok(Command::LogRequest {
        leader_id: log_request.leader_id,
        term: log_request.term,
        prefix_length: log_request.prefix_length,
        prefix_term: log_request.prefix_term,
        leader_commit: log_request.leader_commit,
        suffix: log_request.suffix,
    });
}

#[cfg(test)]
mod tests {
    use crate::command::*;

    #[test]
    fn parse_create_log_command() {
        let command_byte = 0_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "some.log";
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());
        let command = parse_command(&buf);

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
    fn parse_create_log_command_without_log_name() {
        let command_byte = 0_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "some.log";
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_create_log_command_without_log_name_size() {
        let command_byte = 0_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_create_log_command_without_partitions() {
        let command_byte = 0_u8.to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_publish_command() {
        let command_byte = 1_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let content = String::from("event-content");
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());
        buf.extend(content.as_bytes().len().to_be_bytes());
        buf.extend(content.as_bytes());

        let command = parse_command(&buf);

        let mut data = BytesMut::with_capacity(content.len());
        data.put(content.as_bytes());

        assert!(command.is_ok());
        assert_eq!(
            command.unwrap(),
            Command::Publish {
                log_name: "events.log".to_string(),
                partition: 10,
                data,
            }
        )
    }

    #[test]
    fn parse_publish_command_without_data() {
        let command_byte = 1_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let content = String::from("event-content");
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());
        buf.extend(content.as_bytes().len().to_be_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_publish_command_without_data_size() {
        let command_byte = 1_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_publish_command_without_log_name() {
        let command_byte = 1_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_publish_command_without_log_name_size() {
        let command_byte = 1_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_publish_command_without_partitions() {
        let command_byte = 1_u8.to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_fetch_command() {
        let command_byte = 2_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let group_name = String::from("group-name");
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());
        buf.extend((group_name.as_bytes().len() as u8).to_be_bytes());
        buf.extend(group_name.as_bytes());

        let command = parse_command(&buf);

        assert!(command.is_ok());
        assert_eq!(
            command.unwrap(),
            Command::Fetch {
                log_name: "events.log".to_string(),
                partition: 10,
                group: "group-name".to_string(),
            }
        )
    }

    #[test]
    fn parse_fetch_command_without_group_name() {
        let command_byte = 2_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let group_name = String::from("group-name");
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());
        buf.extend((group_name.as_bytes().len() as u8).to_be_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_fetch_command_without_group_name_size() {
        let command_byte = 2_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_fetch_command_without_log_name() {
        let command_byte = 2_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_fetch_command_without_log_name_size() {
        let command_byte = 2_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_fetch_command_without_partitions() {
        let command_byte = 2_u8.to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_ack_command() {
        let command_byte = 3_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let group_name = String::from("group-name");
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());
        buf.extend((group_name.as_bytes().len() as u8).to_be_bytes());
        buf.extend(group_name.as_bytes());

        let command = parse_command(&buf);

        assert!(command.is_ok());
        assert_eq!(
            command.unwrap(),
            Command::Ack {
                log_name: "events.log".to_string(),
                partition: 10,
                group: "group-name".to_string(),
            }
        )
    }

    #[test]
    fn parse_ack_command_without_group_name() {
        let command_byte = 3_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let group_name = String::from("group-name");
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());
        buf.extend((group_name.as_bytes().len() as u8).to_be_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_ack_command_without_group_name_size() {
        let command_byte = 3_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());
        buf.extend(log_name.as_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_ack_command_without_log_name() {
        let command_byte = 3_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let log_name = "events.log";
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);
        buf.extend((log_name.len() as u8).to_be_bytes());

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_ack_command_without_log_name_size() {
        let command_byte = 3_u8.to_be_bytes();
        let partitions = 10_u8.to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);
        buf.extend(partitions);

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_ack_command_without_partitions() {
        let command_byte = 3_u8.to_be_bytes();
        let mut buf = Vec::new();
        buf.extend(command_byte);

        let command = parse_command(&buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_unknown_command() {
        let command_byte = 4_u8.to_be_bytes();
        let command = parse_command(&command_byte);

        assert!(command.is_ok());
        assert_eq!(command.unwrap(), Command::Unknown)
    }
}
