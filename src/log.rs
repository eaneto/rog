use std::os::unix::ffi::OsStrExt;
use std::{collections::HashMap, env, ffi::OsStr, path::Path, sync::Arc, time::SystemTime};

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncReadExt, AsyncWriteExt},
    sync::{
        mpsc::{self, error::SendError, Receiver, Sender},
        Mutex, RwLock,
    },
};
use tracing::{debug, error, trace};

#[derive(Debug)]
pub struct CommitLog {
    pub name: String,
    pub partitions: usize,
    senders: Vec<Sender<InternalMessage>>,
    rog_home: String,
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

        let rog_home = match env::var("ROG_HOME") {
            Ok(path) => path,
            Err(_) => panic!("ROG_HOME environment variable not set"),
        };

        (
            CommitLog {
                name,
                partitions: number_of_partitions,
                senders,
                rog_home,
            },
            receivers,
        )
    }

    pub async fn create_log_files(&self) -> Result<(), &str> {
        match fs::create_dir_all(format!("{}/{}", self.rog_home, self.name)).await {
            Ok(_) => debug!("Successfully created rog and log directories"),
            Err(e) => {
                error!("Unable to create rog and log directory {e}");
                return Err("Unable to create rog and log directory");
            }
        }

        for partition in 0..self.partitions {
            match File::create(format!("{}/{}/{partition}.log", self.rog_home, self.name)).await {
                Ok(_) => debug!(partition = partition, "Successfully created log file"),
                Err(e) => {
                    error!(partition = partition, "Unable to create log file {e}");
                    return Err("Unable to create log file for partition {partition}");
                }
            }
            match File::create(format!("{}/{}/{partition}.id", self.rog_home, self.name)).await {
                Ok(_) => debug!(
                    partition = partition,
                    "Successfully created partition id file"
                ),
                Err(e) => {
                    error!(
                        partition = partition,
                        "Unable to create partition id file {e}"
                    );
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
    pub fn setup_receivers(receivers: Vec<Receiver<InternalMessage>>, log_name: String) {
        let mut ids = Vec::new();
        for _ in 0..receivers.len() {
            ids.push(Mutex::new(0_usize));
        }

        let ids = Arc::new(ids);
        for receiver in receivers {
            let log_name = log_name.clone();
            let ids = ids.clone();
            tokio::spawn(async move {
                let ids = ids.clone();
                let log_receiver = CommitLogReceiver::new(log_name);
                log_receiver.handle_receiver(ids, receiver).await;
            });
        }
    }

    pub async fn fetch_message(&self, partition: usize, group: String) -> Result<BytesMut, &str> {
        self.create_offset_files(&group).await?;

        let mut offset: usize = self.load_offset(partition, &group).await;

        // TODO Instead of loading the whole log in memory this
        // function should only read part of the file, maybe for that
        // I'll need to write a custom serializer instead of using
        // Serde, or split the log in multiple smaller files, kind of
        // how Postgres splits the data in pages of 8kb.
        let log_file_name = format!("{}/{}/{}.log", self.rog_home, self.name, partition);
        let mut log_file = File::open(log_file_name).await.unwrap();
        let mut buf = Vec::new();
        let _ = log_file.read_to_end(&mut buf).await.unwrap();

        let log: HashMap<usize, Message> = if buf.is_empty() {
            HashMap::new()
        } else {
            bincode::deserialize(&buf).unwrap()
        };
        let message = match log.get(&offset) {
            Some(message) => message,
            None => return Err("No data left in the log to be read"),
        };

        // TODO Offset file should only be updated if the response was
        // sent successfully to the client, we have to make sure the
        // client receives the message to update the offset.
        self.increment_and_save_offset(partition, &group, &mut offset)
            .await;
        Ok(message.data.clone())
    }

    async fn create_offset_files(&self, group: &String) -> Result<(), &str> {
        for partition in 0..self.partitions {
            let offset_path = format!("{}/{}/{partition}.{group}.offset", self.rog_home, self.name);
            if Path::new(&offset_path).exists() {
                debug!(partition = partition, "Offset file already exists");
                break;
            }

            match File::create(&offset_path).await {
                Ok(_) => debug!(
                    partition = partition,
                    "Successfully created partition offset file"
                ),
                Err(e) => {
                    error!(
                        partition = partition,
                        "Unable to create partition offset file {e}"
                    );
                    return Err("Unable to create log file for partition {partition}");
                }
            }
        }
        Ok(())
    }

    async fn load_offset(&self, partition: usize, group: &String) -> usize {
        let offset_file_name = format!(
            "{}/{}/{}.{group}.offset",
            self.rog_home, self.name, partition
        );
        let mut offset_file = File::open(&offset_file_name).await.unwrap();
        let mut buf = Vec::new();
        offset_file.read_to_end(&mut buf).await.unwrap();
        if buf.is_empty() {
            0
        } else {
            match bincode::deserialize(&buf) {
                Ok(offset) => offset,
                Err(_) => {
                    debug!(
                        partition = partition,
                        "Offset not found on file, first message consumed on this partition"
                    );
                    0
                }
            }
        }
    }

    async fn increment_and_save_offset(
        &self,
        partition: usize,
        group: &String,
        offset: &mut usize,
    ) {
        let offset_file_name = format!(
            "{}/{}/{}.{group}.offset",
            self.rog_home, self.name, partition
        );
        *offset += 1;
        let binary_offset = bincode::serialize(&offset).unwrap();
        let mut offset_file = OpenOptions::new()
            .write(true)
            .open(&offset_file_name)
            .await
            .unwrap();
        offset_file.write_all(&binary_offset).await.unwrap();
    }
}

pub struct CommitLogReceiver {
    name: String,
    rog_home: String,
}

impl CommitLogReceiver {
    pub fn new(name: String) -> CommitLogReceiver {
        let rog_home = match env::var("ROG_HOME") {
            Ok(path) => path,
            Err(_) => panic!("ROG_HOME environment variable not set"),
        };

        CommitLogReceiver { name, rog_home }
    }

    async fn handle_receiver(
        &self,
        ids: Arc<Vec<Mutex<usize>>>,
        mut receiver: Receiver<InternalMessage>,
    ) {
        // TODO Micro-batching
        while let Some(internal_message) = receiver.recv().await {
            let partition = internal_message.partition;
            let id = self.load_most_recent_id(&ids, partition).await;
            let message = Message::new(id, internal_message);

            let mut log = self.load_log_from_file(partition).await;
            log.insert(message.id, message);

            self.save_log_to_file(log, partition).await;

            self.save_id_file(id, partition).await;
        }
    }

    async fn load_most_recent_id(&self, ids: &Arc<Vec<Mutex<usize>>>, partition: usize) -> usize {
        let mut id = ids[partition].lock().await;
        // Checks if the current id has been loaded from
        // the file or if it's the first message received.
        if *id == 0 {
            match self.load_id_from_file(partition).await {
                Ok(saved_id) => {
                    *id = saved_id + 1;
                }
                Err(_) => trace!(
                    partition = partition,
                    "Id not found on file, first message received on this partition"
                ),
            }
        } else {
            *id += 1;
        }
        *id
    }

    async fn load_id_from_file(&self, partition: usize) -> Result<usize, Box<bincode::ErrorKind>> {
        let id_file_name = format!("{}/{}/{}.id", self.rog_home, self.name, partition);
        let mut id_file = File::open(id_file_name).await.unwrap();
        let mut buf = Vec::new();
        let _ = id_file.read_to_end(&mut buf).await.unwrap();
        bincode::deserialize(&buf)
    }

    async fn load_log_from_file(&self, partition: usize) -> HashMap<usize, Message> {
        let log_file_name = format!("{}/{}/{}.log", self.rog_home, self.name, partition);
        let result = File::open(&log_file_name).await;
        let mut log_file = match result {
            Ok(file) => file,
            Err(e) => {
                panic!("Unable to open log file {e}");
            }
        };
        let mut buf = Vec::new();
        log_file.read_to_end(&mut buf).await.unwrap();

        let log: HashMap<usize, Message> = if buf.is_empty() {
            HashMap::new()
        } else {
            bincode::deserialize(&buf).unwrap()
        };
        log
    }

    async fn save_log_to_file(&self, log: HashMap<usize, Message>, partition: usize) {
        let encoded_log = bincode::serialize(&log).unwrap();
        let log_file_name = format!("{}/{}/{}.log", self.rog_home, self.name, partition);
        let result = OpenOptions::new().write(true).open(&log_file_name).await;
        let mut log_file = match result {
            Ok(file) => file,
            Err(e) => {
                panic!("Unable to open log file {e}");
            }
        };
        log_file.write_all(&encoded_log).await.unwrap();
    }

    async fn save_id_file(&self, id: usize, partition: usize) {
        let encoded_id = bincode::serialize(&id).unwrap();
        let id_file_name = format!("{}/{}/{}.id", self.rog_home, self.name, partition);
        let result = OpenOptions::new().write(true).open(id_file_name).await;

        let mut id_file = match result {
            Ok(file) => file,
            Err(e) => {
                panic!("Unable to open id file for writing {e}");
            }
        };
        id_file.write_all(&encoded_id).await.unwrap();
    }
}

#[derive(Debug)]
pub struct InternalMessage {
    partition: usize,
    data: BytesMut,
    timestamp: SystemTime,
}

impl InternalMessage {
    pub fn new(partition: usize, data: BytesMut) -> InternalMessage {
        InternalMessage {
            partition,
            data,
            timestamp: SystemTime::now(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    id: usize,
    data: BytesMut,
    timestamp: SystemTime,
}

impl Message {
    pub fn new(id: usize, internal_message: InternalMessage) -> Message {
        Message {
            id,
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
        let log_name = file_name.to_string_lossy().to_string();
        let partitions = std::fs::read_dir(format!("{rog_home}/{log_name}"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension() == Some(OsStr::from_bytes(b"log")))
            .count();
        debug!(
            partitions = partitions,
            log_name = log_name,
            "Loading log to memory",
        );
        let (commit_log, receivers) = CommitLog::new(log_name.clone(), partitions);
        CommitLog::setup_receivers(receivers, commit_log.name.clone());
        logs.write().await.insert(log_name, commit_log);
    }
}

#[cfg(test)]
mod tests {
    use crate::log::*;
    use std::env::set_var;

    #[test]
    fn create_new_commit_log() {
        let log_name = "log.name".to_string();
        let partitions = 10;

        set_var("ROG_HOME", "~/.rog");
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

        set_var("ROG_HOME", "~/.rog");
        let (commit_log, mut receivers) = CommitLog::new(log_name.clone(), partitions);
        let message = InternalMessage::new(0, BytesMut::new());
        let result = commit_log.send_message(message).await;

        assert!(result.is_ok());

        let message = receivers[0].recv().await.unwrap();
        assert_eq!(message.partition, 0);
        assert_eq!(message.data, BytesMut::new());
        assert!(message.timestamp < SystemTime::now());
    }
}
