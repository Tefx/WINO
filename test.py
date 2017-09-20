from worker import *


if __name__ == "__main__":
    from sys import argv
    client = Worker.client(argv[1])
    print(client.send_file(data=Data(int(argv[3])), target_addr=argv[2]))
