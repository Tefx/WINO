import gevent
from gevent import socket
# import socket
import multiprocessing
from functools import partial, wraps
import json
import struct

load = json.loads
dump = json.dumps


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
        return buf

    def write(self, buf):
        buf = buf.encode("utf-8")
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
            proc = multiprocessing.Process(target=self.handle_let, args=(sock,))
            proc.start()
            # gevent.spawn(self.handle_let, sock)

    def handle_let(self, sock):
        port = Port(sock)
        while True:
            message = port.read()
            if message:
                port.write(self.handle(message))
            else:
                break

    def handle(self, message):
        func, kwargs = load(message)
        f = getattr(self.instance, func, lambda _: None)
        ret = f(**kwargs)
        return dump(ret)


class RPCClient(object):
    def __init__(self, C):
        self.cls = C

    def connect(self, worker_addr, keep_alive=True):
        self.keep_alive = keep_alive
        self.worker_addr = worker_addr
        if self.keep_alive:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.connect(worker_addr)
            self.port = Port(sock)
        else:
            self.port = None

    def shutdown(self):
        if self.port:
            self.port.close()

    def __getattr__(self, func):
        if not self.port:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(addr)
            port = Port(sock)
        else:
            port = self.port

        def call(**kwargs):
            for name, arg in kwargs.items():
                if hasattr(arg, "__dump__"):
                    kwargs[name] = arg.__dump__()
            st = port.write(dump((func, kwargs)))
            if st:
                ret = load(port.read())
                ret_cls = getattr(self.cls, func).__annotations__.get("return")
                if ret_cls: ret = ret_cls.__load__(ret)
                return ret

        return call


def rpc_method(func):
    @wraps(func)
    def wrapped(self, **kwargs):
        for name, arg in kwargs.items():
            var_cls = func.__annotations__.get(name, None)
            if hasattr(var_cls, "__load__"):
                kwargs[name] = var_cls.__load__(arg)
        res = func(self, **kwargs)
        if hasattr(res, "__dump__"):
            res = res.__dump__()
        return res

    return wrapped
