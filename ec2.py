#!/usr/bin/env python3

import scheduler as s
import worker as w
from cluster import Cluster


class EC2Task(s.Task):
    def execute(self):
        worker = w.Worker.client(self.machine.monitor.ip)
        worker.execute(task=w.Task(self.runtime))


class EC2Comm(s.Comm):
    def execute(self):
        worker = w.Worker.client(self.from_task.machine.monitor.ip)
        data = worker.send_to(
            data=w.Data(self.data_size),
            target_addr=self.to_task.machine.monitor.ip)
        print(self, data.statistic)


class EC2Scheduler(s.Scheduler):
    task_cls = EC2Task
    comm_cls = EC2Comm
    ami = "ami-500b7d33"
    sgroup = "sg-c86bc4ae"
    region = "ap-southeast-1"

    def __init__(self, vm_type, **kwargs):
        self.vm_type = vm_type
        super().__init__(**kwargs)

    def prepare_workers(self):
        cluster = Cluster(self.ami, self.sgroup, self.region)
        monitors = cluster.create_monitors(len(self.machines), self.vm_type)
        for monitor, machine in zip(monitors, self.machines):
            machine.monitor = monitor


# if __name__ == "__main__":
    # from sys import argv
    # s = EC2Scheduler("t2.micro", allow_share=False, log=True)
    # s.load(argv[1])
    # s.run()
