import gevent
from gevent import socket
import multiprocessing
from functools import partial, wraps
import json
import struct
import os
import signal
from functools import partial


class Remotable(object):
    state = ()

    def __dump__(self):
        return [getattr(self, s) for s in self.state]

    @classmethod
    def __load__(cls, state):
        ins = cls.__new__(cls)
        for name, value in zip(cls.state, state):
            setattr(ins, name, value)
        return ins

    def __str__(self):
        return "{}<{}>".format(self.__class__.__name__, ",".join(
            "{}={}".format(s, getattr(self, s)) for s in self.state))


def safe_recv(sock, len):
    try:
        buf = sock.recv(len)
        if buf:
            return buf
    except:
        sock.close()
        return False


def safe_send(sock, buf):
    try:
        sock.sendall(buf)
        return True
    except:
        sock.close()
        return False


def try_connect(sock, addr, times, intervals):
    while times:
        try:
            sock.connect(addr)
            break
        except Exception:
            gevent.sleep(intervals)
            times -= 1
    return times


load = json.loads
dump = json.dumps


class Port(object):
    HEADER_STRUCT = ">L"
    HEADER_LEN = struct.calcsize(HEADER_STRUCT)

    def __init__(self, sock):
        self._sock = sock

    def read(self):
        header = safe_recv(self._sock, self.HEADER_LEN)
        if not header: return False
        length = struct.unpack(self.HEADER_STRUCT, header)[0]
        chunks = []
        while length:
            recv = safe_recv(self._sock, length)
            if not recv: return False
            chunks.append(recv)
            length -= len(recv)
        buf = b"".join(chunks).decode("utf-8")
        return load(buf)

    def write(self, buf):
        buf = dump(buf).encode("utf-8")
        msg = struct.pack(self.HEADER_STRUCT, len(buf)) + buf
        return safe_send(self._sock, msg)

    def close(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()


class RPCServer(object):
    def __init__(self, C, *args):
        self.instance = C(*args)

    def run(self, port=0, pipe=None):
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_sock.bind(("", port))
        listen_sock.listen(10000)
        if pipe:
            pipe.put(listen_sock.getsockname()[1])
        else:
            print("Server<{}> started on port {}".format(
                self.instance.__class__.__name__, listen_sock.getsockname()[
                    1]))
        while True:
            sock, _ = listen_sock.accept()
            proc = multiprocessing.Process(
                target=self.handle_let, args=(sock, ))
            proc.start()

    def handle_let(self, sock):
        port = Port(sock)
        while True:
            port.write(os.getpid())
            message = port.read()
            if message:
                port.write(self.handle(message))
            else:
                break

    def handle(self, message):
        func, kwargs = message
        print("calling {}".format(func))
        func = getattr(self.instance, func, NotImplemented)
        for name, arg in kwargs.items():
            var_cls = func.__annotations__.get(name, None)
            if hasattr(var_cls, "__load__"):
                kwargs[name] = var_cls.__load__(arg)
        res = func(**kwargs)
        if hasattr(res, "__dump__"):
            res = res.__dump__()
        return res


class RProc(object):
    def __init__(self, func, port):
        self.func = func
        self.port = port
        self.kwargs = None
        self.let = None
        self.remote_pid = None

    def dump_args(self, kwargs):
        for name, arg in kwargs.items():
            if hasattr(arg, "__dump__"):
                kwargs[name] = arg.__dump__()

    def load_ret(self, ret):
        ret_cls = self.func.__annotations__.get("return")
        if ret_cls: ret = ret_cls.__load__(ret)
        return ret

    def __call__(self, **kwargs):
        self.kwargs = kwargs
        self.let = gevent.getcurrent()
        self.remote_pid = self.port.read()
        self.dump_args(kwargs)
        st = self.port.write((self.func.__name__, kwargs))
        if st:
            msg = self.port.read()
            if msg:
                return self.load_ret(msg)

    def join(self):
        self.wait_for_init()
        self.let.join()

    def wait_for_init(self):
        while not self.let:
            gevent.sleep(0.1)

    @property
    def value(self):
        return self.let.value


class RPCClient(object):
    def __init__(self, C, worker_addr, keep_alive=False):
        self.cls = C
        self.keep_alive = keep_alive
        self.worker_addr = worker_addr
        self.running_set = []
        self.connect()

    @property
    def ip(self):
        return self.worker_addr[0]

    def connect(self):
        try_times = 10
        if self.keep_alive:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if try_connect(sock, self.worker_addr, 10, 1):
                self.port = Port(sock)
            else:
                self.port = None
        else:
            self.port = None

    def shutdown(self):
        if self.port:
            self.port.close()

    def get_port(self, new_port=False):
        if not self.port or new_port:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try_connect(sock, self.worker_addr, 5, 1)
            port = Port(sock)
        else:
            port = self.port
        return port

    def __getattr__(self, func):
        return RProc(getattr(self.cls, func), self.get_port())

    def async_call(self, func, **kwargs):
        rproc = RProc(getattr(self.cls, func), self.get_port(True))
        gevent.spawn(rproc, **kwargs)
        return rproc

    def suspend(self, rproc):
        rproc.wait_for_init()
        return RProc(self.cls.suspend, self.get_port())(pid=rproc.remote_pid)

    def resume(self, rproc):
        rproc.wait_for_init()
        return RProc(self.cls.resume, self.get_port())(pid=rproc.remote_pid)


class RPC(object):
    default_port = 10000

    @classmethod
    def server(cls, *args):
        RPCServer(cls, *args).run(cls.default_port)

    @classmethod
    def client(cls, addr, port=None, keep_alive=True):
        port = port or cls.default_port
        return RPCClient(cls, (addr, port), keep_alive)

    def hello(self):
        return "Hello!"

    def suspend(self, pid):
        os.kill(pid, signal.SIGSTOP)

    def resume(self, pid):
        os.kill(pid, signal.SIGCONT)
