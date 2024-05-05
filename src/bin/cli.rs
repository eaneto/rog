use std::{
    io::{Read, Write},
    net::TcpStream,
};

use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Command {
    CreateLog {
        /// Log name
        #[clap(long, short)]
        name: String,

        /// Number of partitions for the log
        #[clap(long, short)]
        partitions: u8,
    },
    Publish {
        /// Log name
        #[clap(long, short)]
        log_name: String,

        /// Partition to publish the data
        #[clap(long, short)]
        partition: u8,

        /// Data to be published
        #[clap(long, short)]
        data: String,
    },
    Fetch {
        /// Log name
        #[clap(long, short)]
        log_name: String,

        /// Partition to publish the data
        #[clap(long, short)]
        partition: u8,

        /// Consumer group
        #[clap(long, short)]
        group: String,
    },
    StartElection,
}

#[derive(Debug, Parser)]
struct Args {
    /// Rog server address on the format ip:port, e.g.: 127.0.0.1:7878
    #[clap(long, short)]
    address: String,

    #[clap(subcommand)]
    command: Command,
}

fn main() {
    let args = Args::parse();
    let mut stream = match TcpStream::connect(args.address) {
        Ok(stream) => stream,
        Err(e) => panic!("Unable to stablish connection to rog server {e}"),
    };
    match args.command {
        Command::CreateLog { name, partitions } => {
            let command_byte = (0_u8).to_be_bytes();
            let partitions_as_bytes = partitions.to_be_bytes();
            let name_as_bytes = name.as_bytes();

            let mut command = Vec::new();
            command.extend(command_byte);
            command.extend(partitions_as_bytes);
            command.extend((name_as_bytes.len() as u8).to_be_bytes());
            command.extend(name_as_bytes);

            if let Err(e) = stream.write_all(&command) {
                println!("Unable to create log\n{e}");
                return;
            }

            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(_) => {
                    if buf[0] == 0 {
                        println!("Created log {name} with {partitions} partitions");
                    } else {
                        println!("Unable to create log");
                        let error_message = parse_error(&buf);
                        println!("{error_message}");
                    }
                }
                Err(e) => println!("Unable to create log\n{e}"),
            }
        }
        Command::Publish {
            log_name,
            partition,
            data,
        } => {
            let command_byte = (1_u8).to_be_bytes();
            let partitions_as_bytes = partition.to_be_bytes();
            let name_as_bytes = log_name.as_bytes();
            let data_as_bytes = data.as_bytes();

            let mut command = Vec::new();
            command.extend(command_byte);
            command.extend(partitions_as_bytes);
            command.extend((name_as_bytes.len() as u8).to_be_bytes());
            command.extend(name_as_bytes);
            command.extend(data_as_bytes.len().to_be_bytes());
            command.extend(data_as_bytes);

            if let Err(e) = stream.write_all(&command) {
                println!("Unable to publish to log\n{e}");
                return;
            }

            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(_) => {
                    if buf[0] == 0 {
                        println!(
                            "Successfully published to log {log_name} to partition {partition}"
                        );
                    } else {
                        println!("Unable to publish to log");
                        let error_message = parse_error(&buf);
                        println!("{error_message}");
                    }
                }
                Err(e) => println!("Unable to publish to log\n{e}"),
            }
        }
        Command::Fetch {
            log_name,
            partition,
            group,
        } => {
            let command_byte = (2_u8).to_be_bytes();
            let partitions_as_bytes = partition.to_be_bytes();
            let name_as_bytes = log_name.as_bytes();
            let group_as_bytes = group.as_bytes();

            let mut command = Vec::new();
            command.extend(command_byte);
            command.extend(partitions_as_bytes);
            command.extend((name_as_bytes.len() as u8).to_be_bytes());
            command.extend(name_as_bytes);
            command.extend((group_as_bytes.len() as u8).to_be_bytes());
            command.extend(group_as_bytes);

            if let Err(e) = stream.write_all(&command) {
                println!("Unable to fetch log\n{e}");
                return;
            }

            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(_) => {
                    if buf[0] == 0 {
                        let message_size = usize::from_be_bytes(buf[1..9].try_into().unwrap());
                        let response = String::from_utf8_lossy(&buf[9..(9 + message_size)]);
                        println!("{response}");
                    } else {
                        println!("Unable to fetch log");
                        let error_message = parse_error(&buf);
                        println!("{error_message}");
                    }
                }
                Err(e) => println!("Unable to fetch log\n{e}"),
            }
        }
        Command::StartElection => {
            let command_byte = (6_u8).to_be_bytes();
            let mut command = Vec::new();
            command.extend(command_byte);

            if let Err(e) = stream.write_all(&command) {
                println!("Unable to create log\n{e}");
                return;
            }

            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(_) => {
                    if buf[0] == 0 {
                        println!("Election started");
                    } else {
                        println!("Unable to start election");
                        let error_message = parse_error(&buf);
                        println!("{error_message}");
                    }
                }
                Err(e) => println!("Unable to start election\n{e}"),
            }
        }
    }
}

fn parse_error(buf: &[u8]) -> String {
    let message_size = usize::from_be_bytes(buf[1..9].try_into().unwrap());
    let message = &buf[9..(9 + message_size)];
    String::from_utf8_lossy(message).to_string()
}
