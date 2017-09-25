#!/usr/bin/env python3

import scheduler as s
import worker as w
from cluster import Cluster


class EC2Task(s.Task):
    def execute(self):
        self.machine.worker.execute(task=w.Task(self.runtime))


class EC2Comm(s.Comm):
    def execute(self):
        data = self.from_task.machine.worker.send_to(
            data=w.Data(self.data_size),
            target_addr=self.to_task.machine.worker.ip)
        print(self, data.statistic)


class EC2Scheduler(s.Scheduler):
    task_cls = EC2Task
    comm_cls = EC2Comm
    ami = "ami-3cabdb5f"
    sgroup = "sg-c86bc4ae"
    region = "ap-southeast-1"

    def __init__(self, vm_type, **kwargs):
        self.vm_type = vm_type
        super().__init__(**kwargs)

    def prepare_workers(self):
        cluster = Cluster(self.ami, self.sgroup, self.region)
        workers = cluster.create_workers(len(self.machines), self.vm_type)
        for worker, machine in zip(workers, self.machines):
            machine.worker = worker 


if __name__ == "__main__":
    from sys import argv
    s = EC2Scheduler("t2.micro", allow_share=True, log=True)
    s.load(argv[1])
    s.run()
