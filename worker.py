#!/usr/bin/env python3

from time import sleep
import subprocess
from gevent import socket
from math import ceil
from rpcserver import RPC, Remotable


class Task(Remotable):
    state = ["runtime"]

    def __init__(self, runtime):
        self.runtime = runtime

    def execute(self):
        sleep(self.runtime)


class Data(Remotable):
    state = ["size"]

    def __init__(self, size):
        self.size = size

    def send(self, target_addr):
        client = Worker.client(target_addr)
        port = client.start_nc_server()
        cmd = "/bin/dd if=/dev/zero bs=1k count={} | /bin/nc -vq 0 {} {}".format(
            ceil(self.size / 1024), target_addr, port)
        print(cmd)
        proc = subprocess.run([cmd], shell=True)
        print(proc)


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

    def send_file(self, data: Data, target_addr) -> Data:
        data.send(target_addr)
        return data


if __name__ == "__main__":
    Worker.server()
