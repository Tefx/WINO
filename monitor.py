#!/usr/bin/env python3

from rpcserver import RPC
import os.path
import subprocess
import sys


class Monitor(RPC):
    default_port = 10001

    def __init__(self, path):
        self.path = os.path.abspath(path)

    def update_worker(self):
        subprocess.run(["git", "-C", self.path, "pull"])
        self.stop_worker()
        self.start_worker()

    def start_worker(self):
        subprocess.Popen([os.path.join(self.path, "worker.py")])

    def stop_worker(self):
        subprocess.run(["pkill", "-9", "-f", "worker.py"])

    def restart_worker(self):
        self.stop_worker()
        self.start_worker()


if __name__ == "__main__":
    Monitor.server(sys.argv[1])
