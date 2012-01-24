import socket
import errno
import struct
import os.path
import hashlib
import warnings
from collections import namedtuple
from decimal import Decimal

from .core import gethub, Lock, Future
from . import channel
from .util import marker_object


PREPARED_STMT = marker_object("PREPARED_STMT")
PREPARED_PARAMS = marker_object("PREPARED_PARAMS")
PREPARED_COLS = marker_object("PREPARED_COLS")
QUERY = marker_object("QUERY")
QUERY_FIELDS = marker_object("QUERY_FIELDS")
QUERY_ROWDATA = marker_object("QUERY_ROWDATA")
HANDSHAKE = marker_object("HANDSHAKE")
FLAG_BINARY = 0x0080
FLAG_UNSIGNED = 0x0020

OK_PACKET = (bytearray(b'\x00\x00\x00\x02\x00\x00\x00'),)
FIELD_STR = struct.Struct('<HLBHB')


def _read_lcb(buf, pos=0):
    num = buf[pos]
    if num < 251:
        return num, pos+1
    elif num == 251:
        return None, pos+1
    elif num == 252:
        return struct.unpack_from('<H', buf, pos+1)[0], pos+3
    elif num == 253:
        return buf[pos+1] + (buf[pos+2] << 8) + (buf[pos+3] << 16), pos+4
    elif num == 254:
        return struct.unpack_from('<Q', buf, pos+1)[0], pos+9


def _read_lcbytes(buf, pos=0):
    num = buf[pos]
    pos += 1
    if num < 251:
        return buf[pos:pos+num], pos+num
    elif num == 251:
        return None, pos
    elif num == 252:
        num = struct.unpack_from('<H', buf, pos)[0]
        pos += 2
    elif num == 253:
        num = buf[pos] + (buf[pos+1] << 8) + (buf[pos+2] << 16)
        pos += 3
    elif num == 254:
        num = struct.unpack_from('<Q', buf, pos)[0]
        pos += 8
    return buf[pos:pos+num], pos+num


def _read_lcstr(buf, pos=0):
    num = buf[pos]
    pos += 1
    if num < 251:
        pass
    elif num == 251:
        return None, pos
    elif num == 252:
        num = struct.unpack_from('<H', buf, pos)[0]
        pos += 2
    elif num == 253:
        num = buf[pos] + (buf[pos+1] << 8) + (buf[pos+2] << 16)
        pos += 3
    elif num == 254:
        num = struct.unpack_from('<Q', buf, pos)[0]
        pos += 8
    return buf[pos:pos+num].decode('utf-8'), pos+num


def _write_lcbytes(buf, data):
    ln = len(data)
    if ln <= 250:
        buf.append(ln)
    elif ln <= 0xFFFF:
        buf += struct.pack('<H', ln)
    elif ln <= 0xFFFFFF:
        buf.append(ln & 0xFF)
        buf.append((ln >> 8) & 0xFF)
        buf.append(ln >> 16)
    else:
        buf += struct.pack('<Q', ln)
    buf += data


FIELD_MAPPING = {
    0x00: Decimal,
    0x01: int,
    0x02: int,
    0x03: int,
    0x04: float,
    0x05: float,
    0x06: lambda a: None,
    0x09: int,
    0x0d: int,
    0x0f: lambda a: str(a, 'utf-8'),
    0xf6: Decimal,
    0xf7: lambda a: str(a, 'utf-8'),
    0xf8: lambda a: set(s.decode('utf-8').split(',')),
    0xf9: bytes,
    0xfa: bytes,
    0xfb: bytes,
    0xfc: bytes,
    0xfd: lambda a: str(a, 'utf-8'),
    0xfe: lambda a: str(a, 'utf-8'),
    }
FIELD_BIN_READERS = {
    0x01: lambda buf, pos: (buf[pos], pos+1),
    0x02: lambda buf, pos: (struct.unpack_from('<H', buf, pos)[0], pos+2),
    0x03: lambda buf, pos: (struct.unpack_from('<L', buf, pos)[0], pos+4),
    0x04: lambda buf, pos: (struct.unpack_from('<f', buf, pos)[0], pos+4),
    0x05: lambda buf, pos: (struct.unpack_from('<d', buf, pos)[0], pos+8),
    0x08: lambda buf, pos: (struct.unpack_from('<Q', buf, pos)[0], pos+8),
    0x0f: _read_lcstr,
    0xf7: _read_lcstr,
    0xf9: _read_lcbytes,
    0xfa: _read_lcbytes,
    0xfb: _read_lcbytes,
    0xfc: _read_lcbytes,
    0xfd: _read_lcstr,
    0xfe: _read_lcstr,
    }


class MysqlError(Exception):

    def __init__(self, errno, sqlstate, message):
        self.errno = errno
        self.sqlstate = sqlstate
        self.message = message

    def __str__(self):
        return '({}:{}) {}'.format(self.errno, self.sqlstate, self.message)


_Field = namedtuple('_Field', 'catalog db table org_table name org_name'
        ' charsetnr length type flags decimals default')
class Field(_Field):
    __slots__ = ()

    @classmethod
    def parse_packet(cls, packet, pos=0):
        catalog, pos = _read_lcstr(packet, pos)
        db, pos = _read_lcstr(packet, pos)
        table, pos = _read_lcstr(packet, pos)
        org_table, pos = _read_lcstr(packet, pos)
        name, pos = _read_lcstr(packet, pos)
        org_name, pos = _read_lcstr(packet, pos)
        pos += 1
        charset, length, type, flags, decimals \
            = FIELD_STR.unpack_from(packet, pos)
        pos += FIELD_STR.size + 2
        if len(packet) > pos:
            default = _read_lcstr(packet, pos)
        else:
            default = None
        return cls(catalog, db, table, org_table, name, org_name,
            charset, length, type, flags, decimals, default)


class Resultset(object):

    def __init__(self, reply, nfields, extra):
        self.nfields = nfields
        self.extra = extra
        self.fields = [Field.parse_packet(fp) for fp in reply[1:nfields+1]]
        self.reply = reply

    def __iter__(self):
        raise RuntimeError("Can directly iterate only on resultset from"
            " prepared statement. Use dicts() or tuples() methods")

    def dicts(self):
        for rpacket in self.reply[self.nfields+2:-1]:
            row = {}
            pos = 0
            for f in self.fields:
                col, pos = _read_lcbytes(rpacket, pos)
                if col is None:
                    row[f.name] = None
                else:
                    cvt = FIELD_MAPPING.get(f.type)
                    if cvt is None:
                        raise RuntimeError('{} is not supported'.format(f.type))
                    row[f.name] = cvt(col)
            yield row

    def tuples(self):
        buf = []
        for rpacket in self.reply[self.nfields+2:-1]:
            pos = 0
            for f in self.fields:
                col, pos = _read_lcbytes(rpacket, pos)
                cvt = FIELD_MAPPING.get(f.type)
                if cvt is None:
                    raise RuntimeError('{} is not supported'.format(f.type))
                buf.append(cvt(col))
            yield tuple(buf)
            del buf[:]

class BinaryResultset(object):

    def __init__(self, cls, reply, nfields, extra):
        self.row_class = cls
        self.nfields = nfields
        self.extra = extra
        self.fields = [Field.parse_packet(fp) for fp in reply[1:nfields+1]]
        self.reply = reply

    def __iter__(self):
        row = []
        for rpacket in self.reply[self.nfields+2:-1]:
            nbytes = (self.nfields+2+7)//8
            pos = 1+nbytes
            mask = rpacket[1:pos]
            for f in self.fields:
                # TODO(tailhook) check bit mask
                read = FIELD_BIN_READERS.get(f.type)
                if read is None:
                    raise RuntimeError('{} is not supported'.format(f.type))
                val, pos = read(rpacket, pos)
                row.append(val)
            yield self.row_class(*row)
            del row[:]

    def dicts(self):
        for rpacket in self.reply[self.nfields+2:-1]:
            row = {}
            pos = 0
            for f in self.fields:
                read = FIELD_BIN_READERS.get(f.type)
                if read is None:
                    raise RuntimeError('{} is not supported'.format(f.type))
                val, pos = read(rpacket, pos)
                row[f.name] = val
            yield row

    def tuples(self):
        row = []
        for rpacket in self.reply[self.nfields+2:-1]:
            pos = 0
            for f in self.fields:
                read = FIELD_BIN_READERS.get(f.type)
                if read is None:
                    raise RuntimeError('{} is not supported'.format(f.type))
                val, pos = read(rpacket, pos)
                row.append(val)
            yield tuple(row)
            del row[:]


class PreparedStatement(object):

    def __init__(self, text, id, fields, params):
        self.text = text
        self.id = id
        self.bound = False
        self.fields = fields
        self.params = params
        if self.fields:
            self.row_class = namedtuple('Row', [f.name for f in fields])

    def write_binding(self, buf):
        buf += b'\x01'
        for param in self.params:
            buf += struct.pack('<H', param.type)
        return buf


execute_result = namedtuple('ExecuteResult', 'insert_id affected_rows')


class Capabilities(object):

    def __init__(self, num):
        self.long_password = bool(num & 1)
        self.found_rows = bool(num & 2)
        self.long_flag = bool(num & 4)
        self.connect_with_db = bool(num & 8)
        self.no_schema = bool(num & 16)
        self.compress = bool(num & 32)
        self.odbc = bool(num & 64)
        self.local_files = bool(num & 128)
        self.ignore_space = bool(num & 256)
        self.protocol_41 = bool(num & 512)
        self.interactive = bool(num & 1024)
        self.ssl = bool(num & 2048)
        self.ignore_sigpipe = bool(num & 4096)
        self.transactions = bool(num & 8192)
        self.secure_connection = bool(num & 32768)
        self.multi_statements = bool(num & 65536)
        self.multi_results = bool(num & 131072)

    def to_int(self):
        num = 0
        if self.long_password: num |= 1
        if self.found_rows: num |= 2
        if self.long_flag: num |= 4
        if self.connect_with_db: num |= 8
        if self.no_schema: num |= 16
        if self.compress: num |= 32
        if self.odbc: num |= 64
        if self.local_files: num |= 128
        if self.ignore_space: num |= 256
        if self.protocol_41: num |= 512
        if self.interactive: num |= 1024
        if self.ssl: num |= 2048
        if self.ignore_sigpipe: num |= 4096
        if self.transactions: num |= 8192
        if self.secure_connection: num |= 32768
        if self.multi_statements: num |= 65536
        if self.multi_results: num |= 131072
        return num



class Channel(channel.PipelinedReqChannel):
    BUFSIZE = 16384

    def connect(self, host, port, unixsock, user, password, database):
        try:
            return self._connect(host, port, unixsock,
                                 user, password, database)
        except Exception:
            self._alive = False
            raise

    def _connect(self, host, port, unixsock, user, password, database):
        sock = None
        if host == 'localhost':
            if os.path.exists(unixsock):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                addr = unixsock
        if sock is None:
            sock = socket.socket(socket.AF_INET,
                socket.SOCK_STREAM, socket.IPPROTO_TCP)
            addr = (host, port)
        self._sock = sock
        self._sock.setblocking(0)
        fut = Future()
        self._producing.append((HANDSHAKE, fut))
        try:
            self._sock.connect(addr)
        except socket.error as e:
            if e.errno == errno.EINPROGRESS:
                gethub().do_write(self._sock)
            else:
                raise
        self._start()
        handshake, = fut.get()

        assert handshake[0] == 10, "Wrong protocol version {}".format(
            handshake[0])
        prefix, suffix = handshake[0:].split(b'\0', 1)
        self.thread_id, scramble, caplow, self.language, \
        self.status, caphigh, scrlen = struct.unpack_from('<L8sxHBHHB', suffix)
        self.capabilities = Capabilities((caphigh << 16) + caplow)
        assert self.capabilities.protocol_41, "Old protocol is not supported"
        assert self.capabilities.connect_with_db
        self.capabilities.odbc = False
        self.capabilities.compress = False
        self.capabilities.multi_statement = False
        self.capabilities.multi_results = False
        self.capabilities.ssl = False
        self.capabilities.transactions = False
        buf = bytearray(b'\x00\x00\x00\x01')
        buf += struct.pack('<L4sB23s',
            self.capabilities.to_int()&0xFFFF,
            b'\x8f\xff\xff\xff',
            33, # utf-8 character set with general collation
            b'\x00'*23)
        buf += user.encode('ascii')
        buf += b'\x00'
        if password:
            buf += '\x14'
            hash1 = hashlib.sha1(password.encode('ascii')).digest()
            hash2 = hashlib.sha1(scramble
                + hashlib.sha1(hash1).digest()).digest()
            buf += bytes(a^b for a, b in zip(hash1, hash2))
        else:
            buf += b'\x00'
        buf += database.encode('ascii')
        buf += b'\x00'
        value = self.request(buf, HANDSHAKE).get()
        if value[-1] == 0xff:
            self._parse_error(value[-1])
        assert value == OK_PACKET, value

    def sender(self):
        buf = bytearray()

        add_chunk = buf.extend
        wait_write = gethub().do_write
        sock = self._sock

        while True:
            if not buf:
                self.wait_requests()
            wait_write(sock)
            for chunk in self.get_pending_requests():
                ln = len(chunk)-4
                chunk[0] = ln & 0xFF
                chunk[1] = (ln >> 8) & 0xFF
                chunk[2] = (ln >> 16) & 0xFF
                add_chunk(chunk)
            try:
                bytes = sock.send(buf)
            except socket.error as e:
                if e.errno in (errno.EAGAIN, errno.EINTR):
                    continue
                elif e.errno in (errno.EPIPE, errno.ECONNRESET):
                    raise EOFError()
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
        current = []

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
                elif e.errno in (errno.EPIPE, errno.ECONNRESET):
                    raise EOFError()
                else:
                    raise
            while len(buf)-pos >= 4:
                length = buf[pos] + (buf[pos+1] << 8) + (buf[pos+2] << 16)
                if len(buf)-pos < length+4:
                    continue
                num = buf[pos+3]
                ptype = buf[pos+4]
                self.produce(buf[pos+4:pos+length+4])
                pos += length+4

    def produce(self, value):
        if not self._alive:
            raise channel.ShutdownException()
        state = self.__dict__.setdefault('_state', self._producing[0][0])
        if state is HANDSHAKE:
            del self._state
            self._producing.popleft()[1].set((value,))
        elif value[0] == 0xff:
            self._cur_producing.append(value)
            self._do_produce()
        elif state is QUERY:
            if value[0] == 0x00:
                del self._state
                self._producing.popleft()[1].set((value,))
            else:
                self._cur_producing.append(value)
                self._state = QUERY_FIELDS
        elif state is QUERY_FIELDS:
            self._cur_producing.append(value)
            if value[0] == 0xfe:
                self._state = QUERY_ROWDATA
        elif state is QUERY_ROWDATA:
            self._cur_producing.append(value)
            if value[0] == 0xfe:
                self._do_produce()
        elif state is PREPARED_STMT:
            self._cur_producing.append(value)
            cols, params = struct.unpack_from('<5xHH', value)
            if params:
                self._state = PREPARED_PARAMS
            elif cols:
                self._state = PREPARED_COLS
            else:
                self._do_produce()
        elif state is PREPARED_PARAMS:
            self._cur_producing.append(value)
            if value[0] == 0xfe:
                cols, = struct.unpack_from('<5xH', self._cur_producing[0])
                if cols:
                    self._state = PREPARED_COLS
                else:
                    self._do_produce()
        elif state is PREPARED_COLS:
            self._cur_producing.append(value)
            if value[0] == 0xfe:
                self._do_produce()
        else:
            raise NotImplementedError(state)

    def _do_produce(self):
        res = tuple(self._cur_producing)
        del self._cur_producing[:]
        del self._state
        self._producing.popleft()[1].set(res)



class Mysql(object):

    def __init__(self, host='localhost', port=3306,
                       unixsock='/var/run/mysqld/mysqld.sock',
                       user='root', password='', database='test'):
        self._channel = None
        self._channel_lock = Lock()
        self.host = host
        self.port = port
        self.unixsock = unixsock
        self.user = user
        self.password = password
        self.database = database

    def channel(self):
        if not self._channel:
            with self._channel_lock:
                if not self._channel:
                    self._prepared = {}  # empty on reconnect
                    chan = Channel()
                    chan.connect(self.host, self.port,
                        unixsock=self.unixsock,
                        user=self.user, password=self.password,
                        database=self.database)
                    self._channel = chan
        return self._channel

    def execute(self, query):
        chan = self.channel()
        buf = bytearray(b'\x00\x00\x00\x00')
        buf += b'\x03'
        buf += query.encode('utf-8')
        reply = chan.request(buf, QUERY).get()
        if reply[-1][0] == 0xff:
            self._parse_error(reply[-1])
        return self._parse_execute(reply, query)

    def _parse_execute(self, reply, query):
        assert len(reply) == 1, "Use query for queries that return result set"
        reply = reply[0]
        assert reply[0] == 0, reply
        pos = 1
        affected_rows, pos = _read_lcb(reply, pos)
        insert_id, pos = _read_lcb(reply, pos)
        server_status, nwarn = struct.unpack_from('<HH', reply, pos)
        pos += 4
        if nwarn:
            warnings.warn("Query {!r} caused {} warnings"
                .format(query, nwarn))
        return execute_result(insert_id, affected_rows)

    def query(self, query):
        chan = self.channel()
        buf = bytearray(b'\x00\x00\x00\x00\x03')
        buf += query.encode('utf-8')
        reply = chan.request(buf, QUERY).get()
        if reply[-1][0] == 0xff:
            self._parse_error(reply[-1])
        assert reply[0][0] not in (0, 0xFF, 0xFE), \
            "Use execute for statements that does not return a result set"
        nfields, pos = _read_lcb(reply[0], 0)
        if pos < len(reply[0]):
            extra, pos = _read_lcb(reply[0], pos)
        else:
            extra = 0
        return Resultset(reply, nfields, extra)

    def _parse_error(self, packet):
        if packet[3] == 35: # it's '#'
            errno, code = struct.unpack_from('<xHx5s', packet)
            raise MysqlError(errno,
                             code.decode('ascii'),
                             packet[9:].decode('utf-8'))
        else:
            errno, = struct.unpack_from('<xH', packet)
            raise MysqlError(errno, '?', packet[3:].decode('utf-8'))

    def _prepare(self, chan, query):
        buf = bytearray(b'\x00\x00\x00\x00\x16')
        buf += query.encode('utf-8')
        reply = chan.request(buf, PREPARED_STMT).get()
        if reply[-1][0] == 0xff:
            self._parse_error(reply[-1])
        stmt_id, ncols, nparams, nwarn \
            = struct.unpack_from('<xLHHxH', reply[0])
        if nwarn:
            warnings.warn("Query {!r} caused {} warnings"
                .format(query, nwarn))
        params = []
        for pack in reply[1:nparams+1]:
            params.append(Field.parse_packet(pack))
        if nparams:
            fstart = nparams+2
        else:
            fstart = 1
        fields = []
        for pack in reply[fstart:fstart+ncols]:
            fields.append(Field.parse_packet(pack))
        stmt = PreparedStatement(query, stmt_id, fields, params)
        self._prepared[query] = stmt
        return stmt

    def execute_prepared(self, query, *args):
        chan = self.channel()
        stmt = self._prepared.get(query, None)
        if stmt is None:
            stmt = self._prepare(chan, query)

        if len(args) != len(stmt.params):
            raise TypeError("Expected {} parameters got {}".format(
                len(stmt.params), len(args)))
        buf = bytearray(b'\x00\x00\x00\x00\x17')
        buf += struct.pack('<L5x', stmt.id)
        la = len(args)
        for i in range(0, la, 8):
            byte = 0
            for j in range(i, min(i+8, la)):
                if args[j] is None:
                    byte |= 1 << j
            buf.append(byte)
        if not stmt.bound:
            stmt.write_binding(buf)
        for a in args:
            if not isinstance(a, bytes):
                a = str(a).encode('utf-8')
            _write_lcbytes(buf, a)
        reply = chan.request(buf, QUERY).get()
        if reply[-1][0] == 0xff:
            self._parse_error(reply[-1])
        res = self._parse_execute(reply, query)
        stmt.bound = True
        return res

    def query_prepared(self, query, *args):
        chan = self.channel()
        stmt = self._prepared.get(query, None)
        if stmt is None:
            stmt = self._prepare(chan, query)

        if len(args) != len(stmt.params):
            raise TypeError("Wrong number of parameters to prepared statement")
        buf = bytearray(b'\x00\x00\x00\x00\x17')
        buf += struct.pack('<L5x', stmt.id)
        la = len(args)
        for i in range(0, la, 8):
            byte = 0
            for j in range(i, min(i+8, la)):
                if args[j] is None:
                    byte |= 1 << j
            buf.append(byte)
        if not stmt.bound:
            stmt.write_binding(buf)
        for a in args:
            if not isinstance(a, bytes):
                a = str(a).encode('utf-8')
            _write_lcbytes(buf, a)
        reply = chan.request(buf, QUERY).get()
        if reply[-1][0] == 0xff:
            self._parse_error(reply[-1])
        assert reply[0][0] not in (0, 0xFF, 0xFE), \
            "Use execute for statements that does not return a result set"
        nfields, pos = _read_lcb(reply[0], 0)
        if pos < len(reply[0]):
            extra, pos = _read_lcb(reply[0], pos)
        else:
            extra = 0
        stmt.bound = True
        return BinaryResultset(stmt.row_class, reply, nfields, extra)


