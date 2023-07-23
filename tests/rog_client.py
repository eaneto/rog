import os
import signal
import socket as s
import subprocess
import time
from typing import Optional

import psutil

CRLF = "\r\n"


def initialize_rog_server(profile: str, port: int = 7878, args: Optional[str] = None):
    if profile == "docker":
        binary = "rog-server"
    else:
        binary = "./target/release/rog-server"
    command = [binary, "-p", str(port)]
    if args is not None:
        command.extend(args.split(" "))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(0.5)
    return process.pid


def find_and_kill_rog_process():
    # Workaround to kill the running rog process and restart it
    processes = psutil.process_iter()

    name = "rog-server"
    ssache_process = [p for p in processes if name in p.name()][0]
    os.kill(ssache_process.pid, signal.SIGTERM)


def kill_rog_server(pid):
    os.kill(pid, signal.SIGTERM)
    time.sleep(0.5)


class RogClient:
    IP = "127.0.0.1"

    def connect(self, port: int = 7878):
        self.__socket = s.socket(s.AF_INET, s.SOCK_STREAM)
        self.__socket.connect((self.IP, port))

    def create_log(self, log_name: str, partitions: int):
        request = f"0{partitions}{CRLF}{log_name}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def send_message(self, log_name: str, partition: int, data):
        request = f"1{partition}{CRLF}{log_name}{CRLF}{data}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def send_binary_message(self, log_name: str, partition: int, data: bytes):
        request = f"1{partition}{CRLF}{log_name}{CRLF}"
        request_bytes = bytearray(request.encode("utf-8"))
        request_bytes.extend(data)
        request_bytes.extend(CRLF.encode("utf-8"))
        self.__socket.send(request_bytes)
        return self.__socket.recv(1024)

    def fetch_log(self, log_name: str, partition: int, group: str, buffer_size: int = 1024):
        request = f"2{partition}{CRLF}{log_name}{CRLF}{group}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(buffer_size)
