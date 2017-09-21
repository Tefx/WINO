#!/usr/bin/env python3

from time import sleep
import subprocess
import gevent
from gevent import socket
from math import ceil
from rpcserver import RPC, Remotable, try_connect
import os
from timeit import default_timer as timer
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


class Data(Remotable):
    state = ["size", "runtime"]

    def __init__(self, size):
        self.size = size
        self.runtime = None

    def send_to(self, sock):
        start_time = timer()
        fsize = self.size
        fake_data_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "fakedata")
        with open(fake_data_path, "rb") as f:
            while fsize:
                buf_size = fsize if fsize < FILE_UNIT_SIZE else FILE_UNIT_SIZE
                f.seek(0)
                fsize -= sock.sendfile(f, 0, buf_size)
        self.runtime = timer() - start_time

    @property
    def rate(self):
        return self.size / (self.runtime)

    @property
    def statistic(self):
        return ("{:.0f}MB data transferred in {:.2f}s, {:.0f}MB/s".format(
            bin_format(self.size, "MB"), self.runtime,
            bin_format(self.rate, "MB")))


FILE_UNIT_SIZE = 1024 * 1024 * 10
HEADER_STRUCT = ">Q"
HEADER_LEN = struct.calcsize(HEADER_STRUCT)


class Worker(RPC):
    def execution(self, task: Task) -> Task:
        task.execute()
        return task

    def send_to(self, data: Data, target_addr) -> Data:
        client = Worker.client(target_addr)
        port = client.pick_unused_port()
        server = gevent.spawn(client.setup_file_server, port=port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if not try_connect(sock, (target_addr, port), 10, 0.2): return False
        sock.sendall(struct.pack(HEADER_STRUCT, data.size))
        data.send_to(sock)
        sock.close()
        server.join()
        return data

    def pick_unused_port(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("localhost", 0))
        _, port = s.getsockname()
        s.close()
        return port

    def setup_file_server(self, port):
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.bind(("", port))
        listen_sock.listen(100)
        sock, _ = listen_sock.accept()
        header = sock.recv(HEADER_LEN)
        fsize = struct.unpack(HEADER_STRUCT, header)[0]
        buf = memoryview(bytearray(4096))
        while fsize:
            fsize -= sock.recv_into(buf, min(fsize, 4096))
        sock.close()


if __name__ == "__main__":
    Worker.server()
