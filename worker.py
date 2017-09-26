#!/usr/bin/env python3

from time import sleep, time as timer
import subprocess
import gevent
from gevent import socket
from math import ceil
from rpcserver import RPC, Remotable, try_connect
import os
import struct


def bin_format(s, t):
    if t.upper() == "KB":
        return s / 1024
    elif t.upper() == "MB":
        return s / (1024**2)
    elif t.upper() == "GB":
        return s / (1024**3)
    else:
        return s


class Task(Remotable):
    state = ["runtime"]

    def __init__(self, runtime):
        self.runtime = runtime

    def execute(self):
        sleep(self.runtime)


# FILE_UNIT_SIZE = 1024 * 1024 * 10
FILE_UNIT_SIZE = 4096
HEADER_STRUCT = ">Q"
HEADER_LEN = struct.calcsize(HEADER_STRUCT)


class Data(Remotable):
    state = ["size", "runtime"]

    def __init__(self, size):
        self.size = size
        self.runtime = None

    def send_to(self, sock):
        fsize = self.size
        buf = bytearray(FILE_UNIT_SIZE)
        while fsize:
            if fsize < FILE_UNIT_SIZE:
                fsize -= sock.send(buf[:fsize])
            else:
                fsize -= sock.send(buf)

    @property
    def rate(self):
        return self.size / (self.runtime)

    @property
    def statistic(self):
        return ("{:.0f}MB data transferred in {:.2f}s, {:.0f}MB/s".format(
            bin_format(self.size, "MB"), self.runtime,
            bin_format(self.rate, "MB")))


class Worker(RPC):
    def execute(self, task: Task) -> Task:
        task.execute()
        return task

    def send_to(self, data: Data, target_addr) -> Data:
        start_time = timer()
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.bind(("", 0))
        listen_sock.listen(1)
        _, port = listen_sock.getsockname()
        gevent.spawn(self.file_sending_server, listen_sock, data)
        client = Worker.client(target_addr)
        client.receive_file(port=port)
        data.runtime = timer() - start_time
        return data

    def file_sending_server(self, listen_sock, data):
        sock, _ = listen_sock.accept()
        sock.sendall(struct.pack(HEADER_STRUCT, data.size))
        data.send_to(sock)
        sock.close()

    def receive_file(self, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ip, _ = self._port.peer_name
        if not try_connect(sock, (ip, port), 20, 0.5): return False
        header = sock.recv(HEADER_LEN)
        fsize = struct.unpack(HEADER_STRUCT, header)[0]
        buf = memoryview(bytearray(4096))
        while fsize:
            fsize -= sock.recv_into(buf, min(fsize, 4096))
        sock.close()


if __name__ == "__main__":
    Worker.server()
