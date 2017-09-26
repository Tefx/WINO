#!/usr/bin/env python3

import json
from copy import copy
import heapq
import gevent.pool
from timeit import default_timer as timer
from math import ceil
import os.path
from random import gauss


class Task(object):
    def __init__(self, tid, runtime, resources, planned_st, machine):
        self.tid = tid
        self.runtime = runtime
        self.resources = resources
        self.planned_st = planned_st
        self.machine = machine
        self.succs = []
        self.outputs = []
        self.remaining_prevs = 0

    def execute(self):
        gevent.sleep(self.runtime)

    def __repr__(self):
        return "Task<{}>".format(self.tid)


class Comm(object):
    def __init__(self, from_task, to_task, data_size, planned_st, planned_ft):
        self.from_task = from_task
        self.to_task = to_task
        self.data_size = data_size
        self.planned_st = planned_st
        self.planned_ft = planned_ft

    def execute(self):
        rate = 125829120
        gevent.sleep(ceil(self.data_size / rate))

    def suspend(self):
        pass

    def resume(self):
        pass

    def __repr__(self):
        return "COMM<{}=>{}>".format(self.from_task.tid, self.to_task.tid)


class Machine(object):
    def __init__(self, capacities):
        self.remaining_resources = copy(capacities)
        self.suspended_sending = []
        self.suspended_receiving = []
        self.current_receiving = None
        self.current_sending = None

    def add_sending_comm(self, comm):
        if self.current_sending:
            self.current_sending.suspend()
            self.suspended_sending.append(self.current_sending)
        self.current_sending = comm

    def finish_sending_comm(self):
        if self.suspended_sending:
            self.current_sending = self.suspended_sending.pop()
            self.current_sending.resume()
        else:
            self.current_sending = None

    def add_receiving_comm(self, comm):
        if self.current_receiving:
            self.current_receiving.suspend()
            self.suspended_receiving.append(self.current_receiving)
        self.current_receiving = comm

    def finish_receiving_comm(self):
        if self.suspended_receiving:
            self.current_receiving = self.suspended_receiving.pop()
            self.current_receiving.resume()
        else:
            self.current_receiving = None

    def remove_resources(self, resources):
        self.remaining_resources[0] -= resources[0]
        self.remaining_resources[1] -= resources[1]

    def add_resources(self, resources):
        self.remaining_resources[0] += resources[0]
        self.remaining_resources[1] += resources[1]


class Scheduler(object):
    task_cls = Task
    comm_cls = Comm

    def __init__(self, allow_share=False, allow_preemptive=False, log=False):
        self.allow_share = allow_share
        self.allow_preemptive = not allow_share and allow_preemptive
        self.log = log

    def load(self, path):
        self.alg_name = os.path.basename(path)[:-9]
        with open(path) as f:
            raw_schedule = json.load(f)
        self.num_tasks = raw_schedule["num_tasks"]
        num_machines = len(raw_schedule["machines"])
        capacities = raw_schedule["vm_capacities"]

        self.tasks = {}
        self.machines = []
        for raw_machine in raw_schedule["machines"]:
            machine = Machine(capacities)
            self.machines.append(machine)
            for raw_task in raw_machine:
                tid = raw_task["id"]
                self.tasks[tid] = self.task_cls(
                    tid, raw_task["runtime"], raw_task["resources"],
                    raw_task["start_time"], machine)

        for raw_machine in raw_schedule["machines"]:
            for raw_task in raw_machine:
                task = self.tasks[raw_task["id"]]
                for sid in raw_task["succs"]:
                    task.succs.append(self.tasks[sid])
                    self.tasks[sid].remaining_prevs += 1
                for comm in raw_task["output"]:
                    to_task = self.tasks[comm["to_task"]]
                    to_task.remaining_prevs += 1
                    data = self.comm_cls(task, to_task, comm["data_size"],
                                         comm["start_time"],
                                         comm["finish_time"])
                    task.outputs.append(data)

    def exec_task(self, task):
        if self.log: print("[S][{:.2f}s]{}".format(timer() - self.RST, task))
        task.execute()
        self.remaining_tasks -= 1
        task.machine.add_resources(task.resources)

        for t in task.succs:
            t.remaining_prevs -= 1
            if not t.remaining_prevs:
                self.ready_tasks.add(t)
        for c in task.outputs:
            self.ready_comms.add(c)
        if self.log:
            print("[F][{:.2f}s]{}[{}/{}]".format(
                timer() - self.RST, task, self.num_tasks -
                self.remaining_tasks, self.num_tasks))

    def exec_comm(self, comm):
        if self.log: print("[S][{:.2f}s]{}".format(timer() - self.RST, comm))
        from_task = comm.from_task
        to_task = comm.to_task

        comm.execute()
        if not self.allow_share:
            from_task.machine.finish_sending_comm()
            to_task.machine.finish_receiving_comm()

        to_task.remaining_prevs -= 1
        if not to_task.remaining_prevs:
            self.ready_tasks.add(comm.to_task)
        if self.log: print("[F][{:.2f}s]{}".format(timer() - self.RST, comm))

    def comm_is_ready(self, comm):
        if self.allow_share:
            return True
        elif self.allow_preemptive:
            for current_comm in [
                    comm.from_task.machine.current_sending,
                    comm.to_task.machine.current_receiving
            ]:
                current_comm = comm.from_task.machine.current_sending
                if current_comm and current_comm.planned_ft < comm.planned_ft:
                    return False
            return True
        else:
            return not (comm.from_task.machine.current_sending
                        or comm.to_task.machine.current_receiving)

    def task_is_ready(self, task):
        return all(
            x >= y
            for x, y in zip(task.machine.remaining_resources, task.resources))

    def schedule(self):
        current_time = timer() - self.RST
        for t in sorted(self.ready_tasks, key=lambda t: t.planned_st):
            if t.planned_st > current_time:
                break
            elif self.task_is_ready(t) and t in self.ready_tasks:
                self.ready_tasks.remove(t)
                t.machine.remove_resources(t.resources)
                self.group.spawn(self.exec_task, t)
        for c in sorted(self.ready_comms, key=lambda c: c.planned_st):
            if c.planned_st > current_time:
                break
            elif self.comm_is_ready(c) and c in self.ready_comms:
                self.ready_comms.remove(c)
                if not self.allow_share:
                    c.from_task.machine.add_sending_comm(c)
                    c.to_task.machine.add_receiving_comm(c)
                self.group.spawn(self.exec_comm, c)

    def prepare_workers(self):
        pass

    def run(self):
        self.remaining_tasks = self.num_tasks
        self.prepare_workers()
        self.RST = timer()
        self.ready_tasks = set(
            [t for t in self.tasks.values() if not t.remaining_prevs])
        self.ready_comms = set()
        self.group = gevent.pool.Group()
        while self.remaining_tasks:
            self.schedule()
            gevent.sleep(0.1)
        print("Makespan of {}: {:.2f}s".format(self.alg_name,
                                               timer() - self.RST))


if __name__ == "__main__":
    from sys import argv
    s = Scheduler(allow_share=True, log=True)
    for path in argv[1:]:
        s.load(path)
        s.run()
