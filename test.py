from worker import *
from monitor import *
from cluster import *
from gevent import sleep
from timeit import default_timer as timer

def test_monitor_and_worker():
    from sys import argv
    monitor = Monitor.client(argv[1])
    monitor.start_worker(update=False)

    client = Worker.client(argv[1])
    print(client.hello())
    # sleep(4)
    # monitor.start_worker(update=True)
    # client = Worker.client(argv[1])
    # print(client.hello())
    print(client.send_file(data=Data(int(argv[3])), target_addr=argv[2]))


def test_cluster():
    cluster = Cluster("ami-500b7d33", "sg-c86bc4ae")
    w0, = cluster.create_workers(1)

    start_time = timer()
    print(w0.hello(), w0.ip)
    print(w0.execution_task(task=Task(5)))
    print(w0.send_file(data=Data(102400000000), target_addr=w0.ip))
    print(w0.execution_task(task=Task(2)))
    print("Makespan: {:.2f}s".format(timer() - start_time))


if __name__ == "__main__":
    # test_monitor_and_worker()
    test_cluster()
