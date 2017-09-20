#!/usr/bin/env python3

from rpcserver import RPC
import os.path
import subprocess
import sys


class Monitor(RPC):
    default_port = 10001

    def __init__(self, path):
        self.path = os.path.abspath(path)

    def start_worker(self, update):
        if update:
            subprocess.run(["git", "-C", self.path, "pull"])
        self.stop_worker()
        subprocess.Popen([os.path.join(self.path, "worker.py")])

    def stop_worker(self):
        subprocess.run(["pkill", "-9", "-f", "worker.py"])


if __name__ == "__main__":
    Monitor.server(sys.argv[1])
