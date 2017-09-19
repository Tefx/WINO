from rpcserver import *
from time import sleep
import json

from worker import Worker, Task, Data


class TestState(object):
    def __init__(self, state):
        self.state = state

    def __str__(self):
        return "STATE<{}>".format(self.state)

    def __dump__(self):
        return self.state

    @classmethod
    def __load__(cls, buf):
        return cls(buf)


class TestTask(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, runtime):
        sleep(runtime)
        return TestState("Finished")

    def __dump__(self):
        return json.dumps(self.name)

    @classmethod
    def __load__(cls, buf):
        return cls(json.loads(buf))


class TestWorker(object):
    @rpc_method
    def run_task(self, task: TestTask, runtime) -> TestState:
        return task(runtime)


if __name__ == "__main__":
    from sys import argv
    if argv[1] == "s":
        RPCServer(Worker).run(8888)
    elif argv[1] == "c":
        client = RPCClient(Worker)
        client.connect(("localhost", 8888))
        # print(client.execution_task(task=Task(3)))
        print(client.send_file(data=Data(10240000000), addr=("localhost", 8888)))
