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
        command_byte = (0).to_bytes(1, byteorder="big")
        partitions_bytes = partitions.to_bytes(1, byteorder="big")
        log_name_size = len(log_name).to_bytes(1, byteorder="big")
        log_name_bytes = log_name.encode("utf-8")

        request = bytearray()
        request.extend(command_byte)
        request.extend(partitions_bytes)
        request.extend(log_name_size)
        request.extend(log_name_bytes)
        self.__socket.send(request)
        return self.__socket.recv(1024)

    def send_message(self, log_name: str, partition: int, data: str):
        command_byte = (1).to_bytes(1, byteorder="big")
        partition_bytes = partition.to_bytes(1, byteorder="big")
        log_name_size = len(log_name).to_bytes(1, byteorder="big")
        log_name_bytes = log_name.encode("utf-8")
        data_size = len(data).to_bytes(8, byteorder="big")
        data_bytes = data.encode("utf-8")

        request = bytearray()
        request.extend(command_byte)
        request.extend(partition_bytes)
        request.extend(log_name_size)
        request.extend(log_name_bytes)
        request.extend(data_size)
        request.extend(data_bytes)
        self.__socket.send(request)
        return self.__socket.recv(1024)

    def send_binary_message(self, log_name: str, partition: int, data: bytes):
        command_byte = (1).to_bytes(1, byteorder="big")
        partition_bytes = partition.to_bytes(1, byteorder="big")
        log_name_size = len(log_name).to_bytes(1, byteorder="big")
        log_name_bytes = log_name.encode("utf-8")
        data_size = len(data).to_bytes(8, byteorder="big")

        request = bytearray()
        request.extend(command_byte)
        request.extend(partition_bytes)
        request.extend(log_name_size)
        request.extend(log_name_bytes)
        request.extend(data_size)
        request.extend(data)
        self.__socket.send(request)
        return self.__socket.recv(1024)

    def fetch_log(self, log_name: str, partition: int, group: str, buffer_size: int = 1024):
        command_byte = (2).to_bytes(1, byteorder="big")
        partition_bytes = partition.to_bytes(1, byteorder="big")
        log_name_size = len(log_name).to_bytes(1, byteorder="big")
        log_name_bytes = log_name.encode("utf-8")
        group_size = len(group).to_bytes(1, byteorder="big")
        group_bytes = group.encode("utf-8")

        request = bytearray()
        request.extend(command_byte)
        request.extend(partition_bytes)
        request.extend(log_name_size)
        request.extend(log_name_bytes)
        request.extend(group_size)
        request.extend(group_bytes)
        self.__socket.send(request)
        return self.__socket.recv(buffer_size)
