import struct
from binascii import hexlify
from itertools import count

_unpack_dict = {}
_pack_dict = {}


class ObjectID(bytes):

    def __repr__(self):
        return 'ObjectID.fromhex("{}")'.format(hexlify(self).decode('ascii'))


def _unpack(char):
    def register(fun):
        _unpack_dict[char] = fun
        return fun
    return register


def _pack(char, typ):
    def register(fun):
        _pack_dict[typ] = (char, fun)
        return fun
    return register


@_unpack(0x01)
def unpack_float(buf, idx):
    return struct.unpack_from('<d', buf, idx)[0], idx + 8


@_unpack(0x02)
def unpack_string(buf, idx):
    ln, = struct.unpack_from('<i', buf, idx)
    idx += 4
    if buf[idx+ln-1] != 0:
        raise ValueError("String serialization is broken")
    return buf[idx:idx+ln-1].decode('utf-8'), idx+ln


@_unpack(0x03)
def unpack_document(buf, idx):
    doc_len, = struct.unpack_from('<i', buf, idx)
    if buf[idx+doc_len-1]:
        raise ValueError("Wrong trailing byte")
    idx += 4
    obj = {}
    while buf[idx]:
        typ = buf[idx]
        nameend = buf.index(b'\x00', idx+1)
        fun = _unpack_dict.get(typ)
        if fun is None:
            raise ValueError("Wrong value type")
        obj[buf[idx+1:nameend].decode('utf-8')], idx = fun(buf, nameend+1)
    return obj, idx+1


@_unpack(0x04)
def unpack_array(buf, idx):
    doc_len, = struct.unpack_from('<i', buf, idx)
    if buf[idx+doc_len-1]:
        raise ValueError("Wrong trailing byte")
    idx += 4
    obj = []
    for i in count():
        typ = buf[idx]
        if not typ:
            break
        nameend = buf.index(b'\x00', idx+1)
        fun = _unpack_dict.get(typ)
        if fun is None:
            raise ValueError("Wrong value type")
        if int(buf[idx+1:nameend]) != i:
            raise ValueError("Wrong array index")
        val, idx = fun(buf, nameend+1)
        obj.append(val)
    return obj, idx+1


@_unpack(0x07)
def unpack_objectid(buf, idx):
    return ObjectID(buf[idx:idx+12]), idx+12


@_unpack(0x0A)
def unpack_none(buf, idx):
    return None, idx


@_unpack(0x10)
def unpack_int32(buf, idx):
    return struct.unpack_from('<i', buf, idx)[0], idx + 4


@_unpack(0x12)
def unpack_int64(buf, idx):
    return struct.unpack_from('<q', buf, idx)[0], idx + 8


def loads(s):
    if len(s) < 5:
        raise ValueError("Too short string")
    obj, idx = unpack_document(s, 0)
    if idx != len(s):
        if idx < len(s):
            raise ValueError("Premature end of document")
        else:
            raise ValueError("Garbage at end of document")
    return obj


def iter_load_from(buf, offset=0):
    while offset < len(buf):
        obj, offset = unpack_document(buf, offset)
        yield obj


@_pack(0x01, float)
def pack_float(value, buf):
    buf.extend(struct.pack('<d', value))


@_pack(0x02, str)
def pack_string(value, buf):
    val = value.encode('utf-8')
    buf.extend(struct.pack('<i', len(val)+1))
    buf.extend(val)
    buf.append(0)


@_pack(0x03, dict)
def pack_document(value, buf):
    pos = len(buf)
    buf += b'\x00\x00\x00\x00'
    _pack_doc(value.items(), buf)
    buf.append(0)
    struct.pack_into('<i', buf, pos, len(buf) - pos)


@_pack(0x04, list)
def pack_array(value, buf):
    pos = len(buf)
    buf += b'\x00\x00\x00\x00'
    _pack_doc(((str(i), v)
               for i, v in enumerate(value)), buf)
    buf.append(0)
    struct.pack_into('<i', buf, pos, len(buf) - pos)


@_pack(0x07, ObjectID)
def pack_objectid(value, buf):
    assert len(value) == 12, "ObjectID length is not 12"
    buf += value


@_pack(0x0A, type(None))
def pack_none(value, buf):
    """Nothing needed to pack None, type byte is in another place"""


@_pack(0x10, int)
def pack_int(value, buf):
    if value > (1 << 31) or value < -(1 << 31):
        raise NotImplementedError("Long integers not implemented")
    buf.extend(struct.pack('<i', value))


def _pack_doc(pairs, buf):
    for k, v in pairs:
        spec = _pack_dict.get(type(v))
        if spec is None:
            for typ in _pack_dict:
                if isinstance(v, typ):
                    spec = _pack_dict[typ]
                    break
            else:
                raise ValueError("Can't BSONize {}".format(type(v)))
        ch, fun = spec
        buf.append(ch)
        buf.extend(k.encode('utf-8'))
        buf.append(0)
        fun(v, buf)


def dumps(obj):
    buf = bytearray(4)
    _pack_doc(obj.items(), buf)
    buf.append(0)
    struct.pack_into('<i', buf, 0, len(buf))
    return bytes(buf)


def dump_extend(buf, obj):
    pos = len(buf)
    buf += b'\x00\x00\x00\x00'
    _pack_doc(obj.items(), buf)
    buf.append(0)
    struct.pack_into('<i', buf, pos, len(buf)-pos)


def dump_extend_iter(buf, iterable):
    pos = len(buf)
    buf += b'\x00\x00\x00\x00'
    _pack_doc(iterable, buf)
    buf.append(0)
    struct.pack_into('<i', buf, pos, len(buf) - pos)
