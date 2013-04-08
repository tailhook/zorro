import socket
import errno
import struct
import os.path

from ..core import gethub, Lock
from .. import channel
from . import bson
from ..util import setcloexec


OP_REPLY = 1
OP_MSG = 1000
OP_UPDATE = 2001
OP_INSERT = 2002
OP_QUERY = 2004
OP_GET_MORE	= 2005
OP_DELETE = 2006
OP_KILL_CURSORS	= 2007


class MongodbError(Exception):
    pass


class Channel(channel.MuxReqChannel):
    BUFSIZE = 16384

    def __init__(self, host, port, socket_dir='/tmp'):
        super().__init__()
        sock = None
        if host in ('localhost', '127.0.0.1'):
            unix_sock = os.path.join(socket_dir,
                'mongodb-{}.sock'.format(port))
            if os.path.exists(unix_sock):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                addr = unix_sock
        if sock is None:
            sock = socket.socket(socket.AF_INET,
                socket.SOCK_STREAM, socket.IPPROTO_TCP)
            addr = (host, port)
        setcloexec(sock)
        self._sock = sock
        self._sock.setblocking(0)
        try:
            self._sock.connect(addr)
        except socket.error as e:
            if e.errno == errno.EINPROGRESS:
                gethub().do_write(self._sock)
            else:
                raise
        self._counter = 0
        self._start()

    def new_id(self):
        self._counter += 1
        if self._counter >= 1 << 31:
            self._counter -= 1 << 31
        return self._counter

    def sender(self):
        buf = bytearray()

        add_chunk = buf.extend
        wait_write = gethub().do_write
        sock = self._sock

        while True:
            if not buf:
                self.wait_requests()
            wait_write(sock)
            for id, chunk in self.get_pending_requests():
                struct.pack_into('<ii', chunk, 0, len(chunk), id)
                add_chunk(chunk)
            try:
                bytes = sock.send(buf)
            except socket.error as e:
                if e.errno in (errno.EAGAIN, errno.EINTR):
                    continue
                else:
                    raise
            if not bytes:
                raise EOFError()
            del buf[:bytes]

    def receiver(self):
        buf = bytearray()

        sock = self._sock
        wait_read = gethub().do_read
        add_chunk = buf.extend
        pos = 0

        while True:
            if pos*2 > len(buf):
                del buf[:pos]
                pos = 0
            wait_read(sock)
            try:
                bytes = sock.recv(self.BUFSIZE)
                if not bytes:
                    raise EOFError()
                add_chunk(bytes)
            except socket.error as e:
                if e.errno in (errno.EAGAIN, errno.EINTR):
                    continue
                else:
                    raise
            while len(buf)-pos >= 16:
                length, resp_to = struct.unpack_from('<i4xi', buf, pos)
                if len(buf)-pos < length:
                    break
                self.produce(resp_to, buf[pos:pos+length])
                pos += length


class Connection(object):

    def __init__(self, host='127.0.0.1', port=27017, socket_dir='/tmp'):
        self._channel = None
        self._channel_lock = Lock()
        self._databases = {}
        self.host = host
        self.port = port
        self.socket_dir = socket_dir

    def __getitem__(self, db_name):
        try:
            return self._databases[db_name]
        except KeyError:
            res = Database(db_name, self)
            self._databases[db_name] = res
            return res

    def channel(self):
        if not self._channel:
            with self._channel_lock:
                if not self._channel:
                    self._channel = Channel(
                        self.host, self.port,
                        socket_dir=self.socket_dir)
        return self._channel


class Database(object):

    def __init__(self, name, conn):
        self._name = name
        self._conn = conn
        self._collections = {}

    def __getitem__(self, coll_name):
        try:
            return self._collections[coll_name]
        except KeyError:
            res = Collection(self._name, coll_name, self._conn)
            self._collections[coll_name] = res
            return res


class Collection(object):

    def __init__(self, db_name, name, conn):
        self._name = name
        self._db_name = db_name
        self._fullname_cs = ('{}.{}\0'
            .format(self._db_name, self._name)
            .encode('ascii'))
        self._conn = conn

    def insert(self, record):
        flags = 0  # single record, no need to continue on error
        req = bytearray(struct.pack('<12xii', OP_INSERT, flags))
        req += self._fullname_cs
        bson.dump_extend(req, record)
        self._conn.channel().push(req)

    def insert_many(self, records, continue_on_error=True):
        flags = 1 if continue_on_error else 0
        req = bytearray(struct.pack('<12xii', OP_INSERT, flags))
        req += self._fullname_cs
        for record in records:
            bson.dump_extend(req, record)
        self._conn.channel().push(req)

    def query(self, query, *,
        fields=None, skip=0, limit=0,
        slave_ok=False, partial_ok=False):
        flags = 0
        if slave_ok:
            flags |= 1 << 2
        if partial_ok:
            flags |= 1 << 7
        req = bytearray(struct.pack('<12xii', OP_QUERY, flags))
        req += self._fullname_cs
        req += struct.pack('ii', skip, limit)
        bson.dump_extend(req, query)
        if fields is not None:
            if isinstance(fields, (set, frozenset, list, tuple)):
                bson.dump_extend_iter(req, ((k, 1) for k in fields))
            else:
                bson.dump_extend(req, fields)
        res = self._conn.channel().request(req).get()
        op_code, flags, _, start, num = struct.unpack_from('<12xiiqii', res, 0)
        if op_code != OP_REPLY:
            raise MongodbError("Wrong kind of reply returned")
        if start != skip:
            raise MongodbError("Mongodb returned wrong start number")
        docs = bson.iter_load_from(res, 36)  # struct.calcsize('<12xiiqii')
        if flags & (1 << 1):
            raise MongodbError(next(iter(docs))['$err'])
        return docs

    def delete(self, selector, *, single=False):
        flags = 1 if single else 0
        if not selector:
            raise ValueError("Empty selector for delete() use clean()")
        req = bytearray(struct.pack('<12xi4x', OP_DELETE))
        req += self._fullname_cs
        req += struct.pack('<i', flags)
        bson.dump_extend(req, selector)
        self._conn.channel().push(req)

    def clean(self):
        req = bytearray(struct.pack('<12xi4x', OP_DELETE))
        req += self._fullname_cs
        req += b'\x00\x00\x00\x00\x05\x00\x00\x00\x00'
        self._conn.channel().push(req)

    def update(self, selector, data, *, upsert=False, multi=False):
        flags = 0
        if upsert:
            flags |= 1 << 0
        if multi:
            flags |= 1 << 1
        req = bytearray(struct.pack('<12xi4x', OP_UPDATE))
        req += self._fullname_cs
        req += struct.pack('<i', flags)
        bson.dump_extend(req, selector)
        bson.dump_extend(req, data)
        self._conn.channel().push(req)

    def save(self, doc):
        self.update({ '_id': doc['_id'] }, doc, upsert=True)



