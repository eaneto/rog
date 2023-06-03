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
        partitions: usize,
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

const CRLF: &str = "\r\n";

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

            let mut buf = [0u8; 4];
            match stream.read_exact(&mut buf) {
                Ok(_) => {
                    let ok = format!("OK{CRLF}");
                    if buf == ok.as_bytes() {
                        println!("Created log {name} with {partitions} partitions");
                    } else {
                        println!("Unable to create log\n{:?}", buf);
                    }
                }
                Err(e) => println!("Unable to create log\n{e}"),
            }
        }
    }
}
