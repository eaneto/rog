use atoi::atoi;

use bytes::{BufMut, BytesMut};
use tracing::error;

pub const CRLF: &str = "\r\n";

const CREATE_LOG_COMMAND_BYTE: u8 = 48;
const PUBLISH_COMMAND_BYTE: u8 = 49;
const FETCH_COMMAND_BYTE: u8 = 50;

#[derive(Debug, PartialEq)]
pub enum Command {
    Create {
        name: String,
        partitions: usize,
    },
    Publish {
        log_name: String,
        partition: usize,
        data: BytesMut,
    },
    Fetch {
        log_name: String,
        partition: usize,
    },
    Unknown,
}

pub fn parse_command(buf: &[u8]) -> Result<Command, String> {
    match buf[0] {
        CREATE_LOG_COMMAND_BYTE => parse_create_log_command(buf),
        PUBLISH_COMMAND_BYTE => parse_publish_log_command(buf),
        FETCH_COMMAND_BYTE => parse_fetch_log_command(buf),
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

fn parse_fetch_log_command(buf: &[u8]) -> Result<Command, String> {
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

    Ok(Command::Fetch {
        log_name: log_name.to_string(),
        partition,
    })
}

pub fn read_until_delimiter(buf: &[u8], start: usize, end: usize) -> Result<usize, i64> {
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
    use crate::command::*;

    #[test]
    fn parse_create_log_command() {
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
    fn parse_create_log_command_with_negative_partition_number() {
        let buf = "0-1\r\nsome.log\r\n".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_create_log_command_with_invalid_partition_number() {
        let buf = "0aa\r\nsome.log\r\n".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_create_log_command_without_log_name() {
        let buf = "010\r\n".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_create_log_command_without_crlf_after_log_name() {
        let buf = "010\r\nsome.log".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_err());
    }

    #[test]
    fn parse_unknown_command() {
        let buf = "3example".as_bytes();
        let command = parse_command(buf);

        assert!(command.is_ok());
        assert_eq!(command.unwrap(), Command::Unknown)
    }
}
