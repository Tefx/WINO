from time import sleep
import subprocess
from gevent import socket
from math import ceil
from rpcserver import RPCClient, rpc_method


class Task(object):
    def __init__(self, runtime):
        self.runtime = runtime

    def execute(self):
        sleep(self.runtime)

    def __dump__(self):
        return self.runtime

    @classmethod
    def __load__(self, runtime):
        return Task(runtime)


class Data(object):
    def __init__(self, size):
        self.size = size

    def send(self, addr):
        client = RPCClient(Worker)
        client.connect(tuple(addr))
        port = client.start_nc_server()
        cmd = "dd if=/dev/zero bs=1k count={} | nc -q 0 {} {}".format(
            ceil(self.size / 1024), addr[0], port)
        proc = subprocess.run([cmd], shell=True)

    def __dump__(self):
        return self.size

    @classmethod
    def __load__(self, size):
        return Data(size)


class Worker(object):
    def __init__(self):
        self._nc_server = None

    def __del__(self):
        if self._nc_server:
            self._nc_server.terminate()

    @rpc_method
    def execution_task(self, task: Task) -> Task:
        task.execute()
        return task

    def pick_unused_port(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("localhost", 0))
        _, port = s.getsockname()
        s.close()
        return port

    @rpc_method
    def start_nc_server(self):
        nc_port = self.pick_unused_port()
        print(nc_port)
        self._nc_server = subprocess.Popen(
            ["nc", "-l", str(nc_port)], stdout=subprocess.DEVNULL)
        return nc_port

    @rpc_method
    def send_file(self, data: Data, addr) -> Data:
        data.send(addr)
        return data
