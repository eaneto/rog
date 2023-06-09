use std::{collections::HashMap, env, sync::Arc, time::SystemTime};

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncWriteExt},
    sync::{
        mpsc::{self, error::SendError, Receiver, Sender},
        RwLock,
    },
};
use tracing::{debug, error};

#[derive(Debug)]
pub struct CommitLog {
    pub name: String,
    pub partitions: usize,
    senders: Vec<Sender<InternalMessage>>,
}

impl CommitLog {
    pub fn new(
        name: String,
        number_of_partitions: usize,
    ) -> (CommitLog, Vec<Receiver<InternalMessage>>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..number_of_partitions {
            // TODO Revisit buffer size(or make it configurable).
            let (tx, rx) = mpsc::channel(100);
            senders.push(tx);
            receivers.push(rx);
        }
        (
            CommitLog {
                name,
                partitions: number_of_partitions,
                senders,
            },
            receivers,
        )
    }

    pub async fn create_log_files(&self) -> Result<(), &str> {
        let rog_home = match env::var("ROG_HOME") {
            Ok(path) => path,
            Err(_) => return Err("ROG_HOME environment variable not set"),
        };

        match fs::create_dir_all(format!("{rog_home}/{}", self.name)).await {
            Ok(_) => debug!("Successfully created rog and log directories"),
            Err(e) => {
                error!("Unable to create rog and log directory {e}");
                return Err("Unable to create rog and log directory");
            }
        }

        for partition in 0..self.partitions {
            match File::create(format!("{rog_home}/{}/{partition}.log", self.name)).await {
                Ok(_) => debug!("Successfully created log file for partition {partition}"),
                Err(e) => {
                    error!("Unable to create log file for partition {partition} {e}");
                    return Err("Unable to create log file for partition {partition}");
                }
            }
        }

        Ok(())
    }

    pub async fn send_message(
        &self,
        message: InternalMessage,
    ) -> Result<(), SendError<InternalMessage>> {
        self.senders[message.partition].send(message).await
    }

    /// Initialize a list a receivers to run in background tasks.
    pub fn setup_receivers(receivers: Vec<Receiver<InternalMessage>>) {
        for mut receiver in receivers {
            tokio::spawn(async move {
                // TODO Micro-batching
                while let Some(internal_message) = receiver.recv().await {
                    let partition = internal_message.partition;
                    let log_name = internal_message.log_name.clone();
                    let message = Message::new(internal_message);

                    let encoded = bincode::serialize(&message).unwrap();
                    let rog_home = match env::var("ROG_HOME") {
                        Ok(path) => path,
                        Err(_) => panic!("ROG_HOME enrivonment variable not set"),
                    };

                    let log_file_name = format!("{rog_home}/{}/{}.log", log_name, partition);
                    let result = OpenOptions::new().append(true).open(log_file_name).await;
                    let mut log_file = match result {
                        Ok(file) => file,
                        Err(e) => {
                            panic!("Unable to open log file for writing {e}");
                        }
                    };
                    log_file.write_all(&encoded).await.unwrap();
                    debug!(
                        "Received message {:?} on partition {:?} with timestamp {:?}",
                        message.data, partition, message.timestamp
                    );
                }
            });
        }
    }
}

#[derive(Debug)]
pub struct InternalMessage {
    log_name: String,
    partition: usize,
    data: BytesMut,
    timestamp: SystemTime,
}

impl InternalMessage {
    pub fn new(log_name: String, partition: usize, data: BytesMut) -> InternalMessage {
        InternalMessage {
            log_name,
            partition,
            data,
            timestamp: SystemTime::now(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    data: BytesMut,
    timestamp: SystemTime,
}

impl Message {
    pub fn new(internal_message: InternalMessage) -> Message {
        Message {
            data: internal_message.data,
            timestamp: internal_message.timestamp,
        }
    }
}

pub type Logs = Arc<RwLock<HashMap<String, CommitLog>>>;

pub async fn load_logs(logs: Logs) {
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
        CommitLog::setup_receivers(receivers);
        logs.write().await.insert(log_name.to_string(), commit_log);
    }
}

#[cfg(test)]
mod tests {
    use crate::log::*;

    #[test]
    fn create_new_commit_log() {
        let log_name = "log.name".to_string();
        let partitions = 10;

        let (commit_log, receivers) = CommitLog::new(log_name.clone(), partitions);

        assert_eq!(receivers.len(), partitions);
        assert_eq!(commit_log.senders.len(), partitions);
        assert_eq!(commit_log.name, log_name);
        assert_eq!(commit_log.partitions, partitions);
    }

    #[tokio::test]
    async fn send_message_to_partition() {
        let log_name = "log.name".to_string();
        let partitions = 10;

        let (commit_log, mut receivers) = CommitLog::new(log_name.clone(), partitions);
        let message = InternalMessage::new(log_name.clone(), 0, BytesMut::new());
        let result = commit_log.send_message(message).await;

        assert!(result.is_ok());

        let message = receivers[0].recv().await.unwrap();
        assert_eq!(message.log_name, log_name);
        assert_eq!(message.partition, 0);
        assert_eq!(message.data, BytesMut::new());
        assert!(message.timestamp < SystemTime::now());
    }
}
