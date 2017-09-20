#!/usr/bin/env python3

from time import sleep
import subprocess
import gevent
from gevent import socket
from math import ceil
from rpcserver import RPC, Remotable
import os
from timeit import default_timer as timer
import struct


class Task(Remotable):
    state = ["runtime"]

    def __init__(self, runtime):
        self.runtime = runtime

    def execute(self):
        sleep(self.runtime)


FILE_UNIT_SIZE = 1024 * 1024 * 10
HEADER_STRUCT = ">Q"
HEADER_LEN = struct.calcsize(HEADER_STRUCT)


class Data(Remotable):
    state = ["size"]

    def __init__(self, size):
        self.size = size

    def send(self, target_addr):
        client = Worker.client(target_addr)
        port = client.start_nc_server()
        cmd = "dd if=/dev/zero bs=1k count={} | nc -vq 0 {} {}".format(
            ceil(self.size / 1024), target_addr, port)
        # proc = subprocess.run([cmd], shell=True)
        os.system(cmd)

    def send_file(self, target_addr):
        start_time = timer()
        client = Worker.client(target_addr)
        port = client.pick_unused_port()
        server = gevent.spawn(client.setup_file_server, port=port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try_times = 5
        while try_times:
            try:
                sock.connect((target_addr, port))
                break
            except Exception:
                gevent.sleep(1)
                try_times -= 1
        if not try_times: return False
        sock.sendall(struct.pack(HEADER_STRUCT, self.size))
        fsize = self.size
        with open("./fakedata", "rb") as f:
            while fsize:
                buf_size = fsize if fsize < FILE_UNIT_SIZE else FILE_UNIT_SIZE
                f.seek(0)
                fsize -= sock.sendfile(f, 0, buf_size)
        used_time = timer() - start_time
        server.join()
        sock.close()
        print("Data transferred in {:.2f}s, {:.0f}MB/s".format(
            used_time, self.size / (used_time * 1024 * 1024)))


class Worker(RPC):
    def __init__(self):
        self._nc_server = None

    def __del__(self):
        if self._nc_server:
            self._nc_server.terminate()

    def execution_task(self, task: Task) -> Task:
        task.execute()
        return task

    def pick_unused_port(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("localhost", 0))
        _, port = s.getsockname()
        s.close()
        return port

    def start_nc_server(self):
        nc_port = self.pick_unused_port()
        self._nc_server = subprocess.Popen(
            ["nc", "-vl", str(nc_port)], stdout=subprocess.DEVNULL)
        return nc_port

    def setup_file_server(self, port):
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.bind(("", port))
        listen_sock.listen(100)
        sock, _ = listen_sock.accept()
        header = sock.recv(HEADER_LEN)
        fsize = struct.unpack(HEADER_STRUCT, header)[0]
        buf = memoryview(bytearray(4096))
        while fsize:
            fsize -= sock.recv_into(buf, min(fsize,4096))
        sock.close()

    def send_file(self, data: Data, target_addr) -> Data:
        data.send_file(target_addr)
        return data


if __name__ == "__main__":
    Worker.server()
