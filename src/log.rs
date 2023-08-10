use std::{
    collections::HashMap, env, ffi::OsStr, os::unix::prelude::OsStrExt, path::Path, sync::Arc,
    time::SystemTime,
};

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{
        mpsc::{self, error::SendError, Receiver, Sender},
        Mutex, RwLock,
    },
};
use tracing::{debug, error, trace};

#[derive(Debug)]
pub struct CommitLog {
    pub name: String,
    pub partitions: u8,
    senders: Vec<Sender<InternalMessage>>,
    rog_home: String,
}

impl CommitLog {
    pub fn new(
        name: String,
        number_of_partitions: u8,
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
            match fs::create_dir(format!("{}/{}/{partition}", self.rog_home, self.name)).await {
                Ok(_) => debug!(partition = partition, "Successfully created partition file"),
                Err(e) => {
                    error!(partition = partition, "Unable to create partition file {e}");
                    return Err("Unable to create partition file {partition}");
                }
            }

            match File::create(format!("{}/{}/{partition}/0.log", self.rog_home, self.name)).await {
                Ok(_) => debug!(partition = partition, "Successfully created log file"),
                Err(e) => {
                    error!(partition = partition, "Unable to create log file {e}");
                    return Err("Unable to create log file for partition {partition}");
                }
            }
            match File::create(format!("{}/{}/{partition}/id", self.rog_home, self.name)).await {
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
        self.senders[message.partition as usize].send(message).await
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

    pub async fn fetch_message(&self, partition: u8, group: String) -> Result<BytesMut, &str> {
        self.create_offset_files(&group).await?;

        let mut offset: usize = self.load_offset(partition, &group).await;

        let log_file_name = self.find_log_file(offset, partition);

        let mut log_file = File::open(log_file_name).await.unwrap();
        // First id starts as the first 8 bytes of the file.
        let mut id_start = 0;
        // The id is the first 8 bytes followed by the message size,
        // also 8 bytes, so the message size ends at the 16th
        // position.
        let mut message_size_end = 16;
        let message: Record = loop {
            if let Err(e) = log_file.seek(io::SeekFrom::Start(id_start as u64)).await {
                panic!("Unable to seek file at position {id_start} {e}")
            }
            let mut buf = vec![0; 16];
            if let Err(e) = log_file.read_exact(&mut buf).await {
                match e.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        return Err("No data left in the log to be read");
                    }
                    _ => panic!("Unexpected error reading the log file {e}"),
                }
            };

            let message_id = &buf[0..8];
            let message_id = usize::from_be_bytes(message_id.try_into().unwrap());
            let message_size = &buf[8..16];
            let message_size = usize::from_be_bytes(message_size.try_into().unwrap());
            if message_id == offset {
                if let Err(e) = log_file
                    .seek(io::SeekFrom::Start(message_size_end as u64))
                    .await
                {
                    panic!("Unable to seek file at position {message_size_end} {e}")
                }
                let mut buf = vec![0; message_size];
                if let Err(e) = log_file.read_exact(&mut buf).await {
                    panic!("Unable to read log file at byte {message_size_end} {e}");
                }
                break match bincode::deserialize(&buf) {
                    Ok(record) => record,
                    Err(e) => panic!("Unable to deserialize saved message on log {e}"),
                };
            } else {
                id_start += 16 + message_size;
                message_size_end += 16 + message_size;
            }
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
            let offset_path = format!("{}/{}/{partition}/{group}.offset", self.rog_home, self.name);
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

    fn find_log_file(&self, offset: usize, partition: u8) -> String {
        find_log_file_by_id(&self.rog_home, &self.name, offset, partition)
    }

    async fn load_offset(&self, partition: u8, group: &String) -> usize {
        let offset_file_name =
            format!("{}/{}/{partition}/{group}.offset", self.rog_home, self.name);
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

    async fn increment_and_save_offset(&self, partition: u8, group: &String, offset: &mut usize) {
        let offset_file_name =
            format!("{}/{}/{partition}/{group}.offset", self.rog_home, self.name);
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

            let log_file_name = self.find_log_file(id, partition).await;

            let record = Record::new(internal_message);
            let record_in_storage_format = self.build_record_in_storage_format(&record, id);

            self.save_log_to_file(log_file_name, record_in_storage_format)
                .await;

            self.save_id_file(id, partition).await;
        }
    }

    async fn load_most_recent_id(&self, ids: &Arc<Vec<Mutex<usize>>>, partition: u8) -> usize {
        let mut id = ids[partition as usize].lock().await;
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

    async fn load_id_from_file(&self, partition: u8) -> Result<usize, Box<bincode::ErrorKind>> {
        let id_file_name = format!("{}/{}/{partition}/id", self.rog_home, self.name);
        let mut id_file = File::open(id_file_name).await.unwrap();
        let mut buf = Vec::new();
        let _ = id_file.read_to_end(&mut buf).await.unwrap();
        bincode::deserialize(&buf)
    }

    /// Build the message in the format used in storage.
    ///
    /// The first 8 bytes are the message id, stored in the big endian
    /// format. The following 8 bytes are the message size also stored
    /// as a big endian. The other bytes are the actual content of the
    /// message received stored as a [Record], with the data and the
    /// producer timestamp.
    fn build_record_in_storage_format(&self, record: &Record, id: usize) -> Vec<u8> {
        let record = match bincode::serialize(record) {
            Ok(message) => message,
            Err(e) => panic!("Unable to serialize record to binary {e}"),
        };
        let record_size = record.len().to_be_bytes();
        let binary_id = id.to_be_bytes();
        let mut storable_record = Vec::new();
        storable_record.extend(binary_id);
        storable_record.extend(record_size);
        storable_record.extend(record);
        storable_record
    }

    async fn find_log_file(&self, id: usize, partition: u8) -> String {
        let log_file_name = find_log_file_by_id(&self.rog_home, &self.name, id, partition);

        let result = File::open(&log_file_name).await;
        let log_file = match result {
            Ok(file) => file,
            Err(e) => {
                panic!("Unable to open log file {e}");
            }
        };

        // Is current log file full?  This doesn't actually guarantee
        // that the file will have 8kB, because messages are not split
        // in different files, so if two 5kB messages arrive and are
        // written to a new log file the file will have 10kB. But this
        // condition guarantees that the next 5kB message will be
        // written in a new log file.
        if log_file.metadata().await.unwrap().len() >= 8 * 1024 {
            let log_file_name = format!("{}/{}/{partition}/{id}.log", self.rog_home, self.name);
            match File::create(&log_file_name).await {
                Ok(_) => debug!("Created new log file for {id}"),
                Err(e) => panic!("Unable to create new log file at {log_file_name} {e}"),
            }
            log_file_name
        } else {
            log_file_name
        }
    }

    async fn save_log_to_file(&self, log_file_name: String, parsed_message: Vec<u8>) {
        let result = OpenOptions::new().append(true).open(&log_file_name).await;
        let mut log_file = match result {
            Ok(file) => file,
            Err(e) => {
                panic!("Unable to open log file for appending {log_file_name} {e}");
            }
        };
        if let Err(e) = log_file.write_all(&parsed_message).await {
            panic!("Unable to write to log file {log_file_name} {e}");
        }
    }

    async fn save_id_file(&self, id: usize, partition: u8) {
        let encoded_id = bincode::serialize(&id).unwrap();
        let id_file_name = format!("{}/{}/{partition}/id", self.rog_home, self.name);
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

fn find_log_file_by_id(rog_home: &String, name: &String, id: usize, partition: u8) -> String {
    // TODO Keep all entries in memory
    let mut log_files: Vec<usize> = std::fs::read_dir(format!("{}/{}/{partition}", rog_home, name))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension() == Some(OsStr::from_bytes(b"log")))
        .map(|file| {
            file.path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned()
                .strip_suffix(".log")
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .collect();
    log_files.sort();

    let mut log_file_matching_id = None;
    for files in log_files.windows(2) {
        if id >= files[0] && id < files[1] {
            log_file_matching_id = Some(files[0]);
            break;
        }
    }

    match log_file_matching_id {
        Some(file_name) => format!("{}/{}/{partition}/{}.log", rog_home, name, file_name),
        None => format!(
            "{}/{}/{partition}/{}.log",
            rog_home,
            name,
            log_files.last().unwrap()
        ),
    }
}

#[derive(Debug)]
pub struct InternalMessage {
    partition: u8,
    data: BytesMut,
    timestamp: SystemTime,
}

impl InternalMessage {
    pub fn new(partition: u8, data: BytesMut) -> InternalMessage {
        InternalMessage {
            partition,
            data,
            timestamp: SystemTime::now(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    data: BytesMut,
    timestamp: SystemTime,
}

impl Record {
    pub fn new(internal_message: InternalMessage) -> Record {
        Record {
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
        // Counts how many directories are inside the log directory,
        // each partition has its own directory.
        let partitions = std::fs::read_dir(format!("{rog_home}/{log_name}"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().is_dir())
            .count() as u8;

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

        assert_eq!(receivers.len() as u8, partitions);
        assert_eq!(commit_log.senders.len() as u8, partitions);
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
