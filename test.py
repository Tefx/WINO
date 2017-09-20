from worker import *
from monitor import *
from gevent import sleep


if __name__ == "__main__":
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
