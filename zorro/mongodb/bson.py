import struct
from itertools import count

_unpack_dict = {}


def _unpack(char):
    def register(fun):
        _unpack_dict[char] = fun
        return fun
    return register


@_unpack(0x01)
def unpack_float(buf, idx):
    return struct.unpack_from('<d', buf, idx)[0], idx + 8


@_unpack(0x02)
def unpack_string(buf, idx):
    ln, = struct.unpack_from('<i', buf, idx)
    idx += 4
    end = buf.index(b'\x00', idx)
    return buf[idx:end].decode('utf-8'), end+1


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

