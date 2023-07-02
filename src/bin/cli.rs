use std::{
    io::{Read, Write},
    net::TcpStream,
};

use clap::{Parser, Subcommand};
use rog::command::{read_until_delimiter, CRLF};

#[derive(Debug, Subcommand)]
enum Command {
    CreateLog {
        /// Log name
        #[clap(long, short)]
        name: String,

        /// Number of partitions for the log
        #[clap(long, short)]
        partitions: usize,
    },
    Publish {
        /// Log name
        #[clap(long, short)]
        log_name: String,

        /// Partition to publish the data
        #[clap(long, short)]
        partition: usize,

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
        partition: usize,

        /// Consumer group
        #[clap(long, short)]
        group: String,
    },
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
            let command = format!("0{partitions}{CRLF}{name}{CRLF}");
            let command = command.as_bytes();
            if let Err(e) = stream.write_all(command) {
                println!("Unable to create log\n{e}");
                return;
            }

            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(_) => {
                    let ok = format!("+OK{CRLF}");
                    if &buf[..5] == ok.as_bytes() {
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
            let command = format!("1{partition}{CRLF}{log_name}{CRLF}{data}{CRLF}");
            let command = command.as_bytes();
            if let Err(e) = stream.write_all(command) {
                println!("Unable to publish to log\n{e}");
                return;
            }

            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(_) => {
                    let ok = format!("+OK{CRLF}");
                    if &buf[0..5] == ok.as_bytes() {
                        println!(
                            "Successfully published to log {log_name} to partition {partition}"
                        );
                    } else {
                        println!("Unable to publish to log",);
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
            let command = format!("2{partition}{CRLF}{log_name}{CRLF}{group}{CRLF}");
            let command = command.as_bytes();
            if let Err(e) = stream.write_all(command) {
                println!("Unable to fetch log\n{e}");
                return;
            }

            let mut buf = [0; 1024];
            match stream.read(&mut buf) {
                Ok(_) => {
                    let response = String::from_utf8_lossy(&buf);
                    println!("{response}");
                }
                Err(e) => println!("Unable to fetch log\n{e}"),
            }
        }
    }
}

fn parse_error(buf: &[u8]) -> String {
    let start = 1;
    let end = buf.len() - 1;
    let index = match read_until_delimiter(buf, start, end) {
        Ok(index) => index,
        Err(index) => {
            panic!("Unparseable command at index {index}");
        }
    };
    String::from_utf8_lossy(&buf[start..index]).to_string()
}
