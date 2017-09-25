from worker import *
from monitor import *
from cluster import *
from gevent import sleep
from timeit import default_timer as timer


def test_monitor_and_worker():
    from sys import argv
    monitor = Monitor.client(argv[1])
    monitor.start_worker(update=False)

    w = Worker.client(argv[1])
    print(w.hello())
    rproc = w.async_call("execute", task=Task(10))
    data = Data(int(argv[3]))
    data = w.send_to(data=data, target_addr=argv[2])
    w.suspend(rproc)
    sleep(5)
    w.resume(rproc)
    print(data.statistic)
    rproc.join()


def test_cluster():
    cluster = Cluster("ami-500b7d33", "sg-c86bc4ae")
    w0, w1 = cluster.create_workers(2, "t2.micro")

    t1 = Task(5)
    t2 = Task(3)
    d12 = Data(100000000)

    start_time = timer()
    w0.execute(task=t1)
    w1.send_to(data=d12, target_addr=w1.ip)
    w1.execute(task=t1)
    print("Makespan: {:.2f}s".format(timer() - start_time))


if __name__ == "__main__":
    test_monitor_and_worker()
    # test_cluster()
