#!/usr/bin/env python3

import scheduler
import worker
from cluster import Cluster


class EC2Task(scheduler.Task):
    def execute(self):
        self.machine.worker.execute(task=worker.Task(self.runtime))


class EC2Comm(scheduler.Comm):
    def execute(self):
        self.from_task.worker.send_to(
            data=worker.Data(self.data_size),
            target_addr=self.to_task.worker.ip)


class EC2Scheduler(scheduler.Scheduler):
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
        workers = cluster.create_workers(len(self.machines), self.vm_type)
        for worker, machine in zip(workers, self.machines):
            machine.worker = worker


if __name__ == "__main__":
    from sys import argv
    s = EC2Scheduler("t2.micro", allow_share=True, log=True)
    s.load(argv[1])
    s.run()
