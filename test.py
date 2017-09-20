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
    monitor.start_worker(update=True)
    client = Worker.client(argv[1])
    print(client.hello())
    # print(client.send_file(data=Data(int(argv[3])), target_addr=argv[2]))

if __name__ == "__main__":
    cluster = Cluster("ami-fd33459e", "sg-c86bc4ae")
    w0, w1 = cluster.create_workers(2)

    start_time = timer()
    print(w0.hello(), w0.ip)
    print(w1.hello(), w1.ip)
    print(w0.execution_task(task=Task(5)))
    print(w0.send_file(data=Data(1024000000), target_addr=w1.ip))
    print(w1.execution_task(task=Task(2)))
    print(w1.send_file(data=Data(1024000000), target_addr=w0.ip))
    print(w0.execution_task(task=Task(2)))
    print("Makespan: {:.2f}s".format(timer() - start_time))

