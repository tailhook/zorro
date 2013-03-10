import socket
import random
import struct
import os
from collections import namedtuple
import logging
import abc
import time

from .core import gethub, Future, TimeoutError
from . import sleep


log = logging.getLogger(__name__)

type_to_code = {
    'A': b'\x00\x01',
    'NS': b'\x00\x02',
    'CNAME': b'\x00\x05',
    'PTR': b'\x00\x0C',
    'MX': b'\x00\x0F',
    'TXT': b'\x00\x10',
    'SRV': b'\x00\x21',
    }
IN_CLASS = b'\x00\x01'
code_to_type = {v: k for k, v in type_to_code.items()}
Header = namedtuple('Header', 'id flags qdcount ancount nscount arcount')
F_QR = 1 << 15
F_AA = 1 << 10
F_TC = 1 << 9
F_RD = 1 << 8
F_RA = 1 << 7
F_RCODE = 15
POINTER_MASK = 0b11000000


class DNSError(Exception):
    pass


class DNSFormatError(DNSError):
    code = 1


class DNSServerFailure(DNSError):
    code = 2


class DNSNameError(DNSError):
    code = 3


class DNSNotImplemented(DNSError):
    code = 4


class DNSRefused(DNSError):
    code = 5


code_to_error = {
    DNSFormatError.code: DNSFormatError,
    DNSServerFailure.code: DNSServerFailure,
    DNSNameError.code: DNSNameError,
    DNSNotImplemented.code: DNSNotImplemented,
    DNSRefused.code: DNSRefused,
    }


class Record(object):
    __slots__ = ('name', 'ttl')
    type = None
    klass = 'IN'

    def __init__(self, name, ttl):
        self.name = name
        self.ttl = ttl

    @abc.abstractmethod
    def parse_from(self, data, pos, ln):
        pass

    def __repr__(self):
        return '<RR {} {}>'.format(self.type,
            ', '.join(k + '=' + repr(getattr(self, k))
            for k in self.__slots__))


class ARecord(Record):
    __slots__ = ('ip',)
    type = 'A'

    def parse_from(self, data, pos, ln):
        self.ip = socket.inet_ntoa(data[pos:pos+ln])


class MXRecord(Record):
    __slots__ = ('priority', 'server')
    type = 'MX'

    def parse_from(self, data, pos, ln):
        self.priority, = struct.unpack_from('>H', data, pos)
        self.server, _ = _read_name(data, pos+2)


class CNAMERecord(Record):
    __slots__ = ('canonical_name',)
    type = 'CNAME'

    def parse_from(self, data, pos, ln):
        self.canonical_name = _read_name(data, pos)[0]


class NSRecord(Record):
    __slots__ = ('authoritative_name',)
    type = 'NS'

    def parse_from(self, data, pos, ln):
        self.authoritative_name = _read_name(data, pos)[0]


class PTRRecord(Record):
    __slots__ = ('pointer_target',)
    type = 'PTR'

    def parse_from(self, data, pos, ln):
        self.pointer_target = _read_name(data, pos)[0]


class TXTRecord(Record):
    __slots__ = ('text',)
    type = 'TXT'

    def parse_from(self, data, pos, ln):
        self.text = data[pos:pos+ln].decode('utf-8')


class SRVRecord(Record):
    __slots__ = ('priority', 'weight', 'port', 'target')
    type = 'SRV'

    def parse_from(self, data, pos, ln):
        fields = struct.unpack_from('>HHH', data, pos)
        self.priority, self.weight, self.port = fields
        self.target, _ = _read_name(data, pos+6)


rr_types = {
    'A': ARecord,
    'CNAME': CNAMERecord,
    'NS': NSRecord,
    'PTR': PTRRecord,
    'TXT': TXTRecord,
    'MX': MXRecord,
    'SRV': SRVRecord,
    }


def _read_name(data, pos):
    name = []
    while True:
        ln = data[pos]
        if ln & POINTER_MASK == POINTER_MASK:
            off = ((ln & ~POINTER_MASK) << 8) | data[pos+1]
            name.append(_read_name(data, off)[0])
            return '.'.join(name), pos+2
        else:
            pos += 1
            if not ln:
                return '.'.join(name), pos
            part = data[pos:pos+ln].decode('ascii')
        name.append(part)
        pos += ln


class Resolver(object):

    def __init__(self, config, max_ttl=86400):
        self.config = config
        self.max_ttl = max_ttl
        self.cur_requests = {}
        self.udp_requests = {}
        self.cache = {}
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setblocking(0)
        hub = gethub()
        hub.do_spawnhelper(self._receiver)

    def request(self, name, typ):
        key = name, typ
        fut = self.cur_requests.get(key, None)
        if fut is not None:
            return fut
        fut = Future()
        self.cur_requests[key] = fut
        try:
            self._do_resolve(name, typ, fut)
        finally:
            del self.cur_requests[key]
        return fut

    def resolve(self, name, typ='A'):
        if self.config.check():
            self.cache.clear()
        cache = self.config.hosts.get(name, None)
        if cache is not None:
            return [cache]
        cache = self.cache.get((name, typ))
        if cache is not None:
            return cache
        return self.request(name, typ).get()

    def _do_resolve(self, name, typ, fut):
        query = bytearray()
        query.extend(b'\x00\x00'  # dummy request id for now
                     b'\x01\x00'  # flags: recursion only
                     b'\x00\x01'  # only single query
                     b'\x00\x00'  # no answers
                     b'\x00\x00'  # no authority
                     b'\x00\x00') # no additional
        for part in name.split('.'):
            part = part.encode('ascii')
            query.append(len(part))
            query.extend(part)
        query.append(0)
        query.extend(type_to_code[typ])
        query.extend(IN_CLASS)
        ns_to_try = self.config.ns[:]
        for n in ns_to_try:
            rid = random.randrange(65536)
            struct.pack_into('>H', query, 0, rid)
            key = rid, name, typ
            self.udp_requests[key] = fut
            try:
                try:
                    self._sock.sendto(query, (n, 53))
                except OSError as e:
                    continue
                try:
                    fut.get(timeout=self.config.options['timeout'])
                except TimeoutError:
                    continue
                else:
                    break
            finally:
                del self.udp_requests[key]
        if fut.check():
            fut.throw(DNSRefused("No server available"))

    def _receiver(self):
        while True:
            gethub().do_read(self._sock)
            try:
                data, (host, port) = self._sock.recvfrom(512)
            except OSError:
                continue
            if port != 53 or host not in self.config.ns:
                continue  # some garbage received
            try:
                packet = self._parse_packet(data)
            except Exception as e:
                log.exception('Error parsing DNS packet', exc_info=e)
                continue
            if packet is None:
                continue

    def _parse_packet(self, data):
        head = Header(*struct.unpack_from('>6H', data))
        if not (head.flags & F_QR):
            return  # got question ?
        if head.qdcount != 1:
            return  # we don't ask several questions at once
        name, pos = _read_name(data, 12)
        typ = code_to_type.get(data[pos:pos+2], None)
        if typ is None:
            return  # unsupported type
        if data[pos+2:pos+4] != IN_CLASS:
            return  # we only request IN class
        pos += 4
        fut = self.udp_requests.get((head.id, name, typ), None)
        if fut is None or not fut.check():
            return  # late or unsolicited reply
        err = head.flags & F_RCODE
        if err:
            exc = code_to_error.get(err)
            if exc is not None:
                fut.throw(exc())
            else:
                fut.throw(DNSError("Resolving error: {}".format(err)))
            return
        rrs = []
        for i in range(head.ancount):
            rr, pos = self._read_rr(data, pos)
            rrs.append(rr)
        fut.set(rrs)

    @staticmethod
    def _read_rr(data, pos):
        name, pos = _read_name(data, pos)
        typ = code_to_type.get(data[pos:pos+2])
        if typ is None:
            raise ValueError("Wrong RR type {!r}".format(data[pos:pos+2]))
        cls = data[pos+2:pos+4]
        assert cls == IN_CLASS
        pos += 4
        ttl, ln = struct.unpack_from('>LH', data, pos)
        pos += 6
        cls = rr_types[typ]
        rr = cls(name, ttl)
        rr.parse_from(data, pos, ln)
        return rr, pos+ln

    # CONVENTIONAL FUNCTIONS

    def gethostbyname(self, name):
        records = self.resolve(name)
        by_name = {}
        for rec in records:
            if rec.name == name and isinstance(rec, ARecord):
                return rec.ip
            by_name[rec.name] = rec
        while True:
            rec = by_name.get(name, None)
            if isinstance(rec, CNAMERecord):
                name = rec.canonical_name
            elif isinstance(rec, ARecord):
                return rec.ip
            else:
                raise DNSServerFailure(records)


class Config(object):

    def __init__(self, check_files_time=1):
        self.check_files_time = check_files_time
        self.files_to_check = {}
        self.clear()

    def clear(self):
        self.hosts = {}
        self.ns = []
        self.search = []
        # self.sortlist = []  # TODO(tailhook) too rare to implement now
        self.options = {
            'ndots': 1,
            'timeout': 1,  # what is a good timeout?
            'attempts': 2,
            'rotate': False,
            }

    def check(self):
        if self.last_check + self.check_files_time < time.time():
            for fn, mtime in self.files_to_check.items():
                if os.path.getmtime(fn) != mtime:
                    break
            else:
                return False
            self.clear()
            self.load_from_files(
                hosts=self.hosts_file,
                resolv=self.resolv_file)
            return True
        return False

    @classmethod
    def system_config(cls):
        self = cls()
        self.load_from_files()
        return self

    def load_from_files(self, hosts='/etc/hosts', resolv='/etc/resolv.conf'):
        self.last_check = time.time()
        self.hosts_file = hosts
        self.resolv_file = resolv
        self.read_hosts_file(hosts)
        self.read_resolv_file(resolv)
        self.apply_environment()

    def read_hosts_file(self, fn='/etc/hosts'):
        with open(fn, 'rt') as f:
            self.files_to_check[fn] = os.path.getmtime(fn)
            for line in f:
                line = line.strip()
                if not line or line.startswith((';', '#')):
                    continue
                ip, *hosts = line.split()
                for h in hosts:
                    self.hosts[h] = ip

    def read_resolv_file(self, fn='/etc/resolv.conf'):
        with open(fn, 'rt') as f:
            self.files_to_check[fn] = os.path.getmtime(fn)
            for line in f:
                line = line.strip()
                if line.startswith((';', '#')):
                    continue
                option, arg = line.split(None, 1)
                if option == 'nameserver':
                    self.ns.append(arg)
                elif option == 'domain':
                    self.search = [arg]
                elif option == 'search':
                    self.search.append(arg)
                elif option == 'options':
                    self.parse_options(arg)
                else:
                    warnings.warn('Wrong option {!r} in {!r}'.format(
                        option, fn))

    def apply_environment(self, env=os.environ):
        if 'LOCALDOMAIN' in env:
            self.search = env['LOCALDOMAIN'].split()
        if 'RES_OPTIONS' in env:
            self.options.update(self.parse_options(env['RES_OPTIONS']))

    def parse_options(self, optstr):
        for word in optstr.split():
            if ':' in word:
                key, val = word.split(':', 1)
                self.options[key] = int(val)
            else:
                self.options[word] = True

