#!/usr/bin/env python3

import scheduler as s
import worker as w
from cluster import Cluster
from gevent import sleep


class EC2Task(s.Task):
    def execute(self):
        self.machine.worker.execute(task=w.Task(self.runtime))


class EC2Comm(s.Comm):
    def execute(self):
        self.rproc = self.from_task.machine.worker.async_call(
            "send_to",
            data=w.Data(self.data_size),
            target_addr=self.to_task.machine.worker.private_ip)
        self.rproc.join()
        print(self, self.rproc.value.statistic)

    def wait_for_init(self):
        while not hasattr(self, "rproc"):
            sleep(0.01)

    def suspend(self):
        self.wait_for_init()
        self.from_task.machine.worker.suspend(self.rproc)

    def resume(self):
        self.from_task.machine.worker.resume(self.rproc)


class EC2Scheduler(s.Scheduler):
    task_cls = EC2Task
    comm_cls = EC2Comm
    ami = "ami-3cabdb5f"
    sgroup = "sg-c86bc4ae"
    pgroup = "wino"
    region = "ap-southeast-1"

    def __init__(self, vm_type, **kwargs):
        self.vm_type = vm_type
        super().__init__(**kwargs)

    def prepare_workers(self):
        cluster = Cluster(self.ami, self.sgroup, self.region, self.pgroup)
        workers = cluster.create_workers(len(self.machines), self.vm_type)
        for worker, machine in zip(workers, self.machines):
            machine.worker = worker


if __name__ == "__main__":
    from sys import argv
    s = EC2Scheduler(
        # "t2.micro", allow_share=False, allow_preemptive=False, log=True)
        "c4.large", allow_share=False, allow_preemptive=True, log=True)
    s.load(argv[1])
    s.run()
