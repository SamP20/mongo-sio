import struct
import zlib
import bson
import platform
import itertools
from typing import Dict, Any, List, Optional, Callable

try:
    import snappy
    SNAPPY_SUPPORTED = True
except ImportError:
    SNAPPY_SUPPORTED = False

OP_QUERY = 2004
OP_REPLY = 1
OP_COMPRESSED = 2012
OP_MSG = 2013

COMPRESSOR_NOOP = 0
COMPRESSOR_SNAPPY = 1
COMPRESSOR_ZLIB = 2
COMPRESSOR_ZSTD = 3

SUPPORTED_COMPRESSORS = {COMPRESSOR_NOOP}
if SNAPPY_SUPPORTED:
    SUPPORTED_COMPRESSORS.add(COMPRESSOR_SNAPPY)
SUPPORTED_COMPRESSORS.add(COMPRESSOR_ZLIB)

REQUEST_ID_COUNTER = itertools.cycle(range(0x7fff_ffff))

def none_compress(op_code: int, request_id: int, response_to: int, data_fragments: List[bytes]):
    total_len = sum(len(x) for x in data_fragments) + 16
    data_fragments = [struct.pack("<iiii", total_len, request_id, response_to, op_code)] + data_fragments
    return b"".join(data_fragments)

def zlib_compressor(level: int=6):
    def zlib_compress(op_code: int, request_id: int, response_to: int, data_fragments: List[bytes]):
        uncompressed_size = sum(len(x) for x in data_fragments)
        compressed_msg = zlib.compress(b"".join(data_fragments), level=level)
        compressed_size = len(compressed_msg)
        message_length = compressed_size + 16 + 8 + 1
        return struct.pack(f"<iiiiiiB{compressed_size}s", message_length, request_id, response_to, OP_COMPRESSED, op_code, uncompressed_size, COMPRESSOR_ZLIB, compressed_msg)
    return zlib_compress

def snappy_compress(op_code: int, request_id: int, response_to: int, data_fragments: List[bytes]):
    uncompressed_size = sum(len(x) for x in data_fragments)
    compressed_msg = snappy.compress(b"".join(data_fragments))
    compressed_size = len(compressed_msg)
    message_length = compressed_size + 16 + 8 + 1
    return struct.pack(f"<iiiiiiB{compressed_size}s", message_length, request_id, response_to, OP_COMPRESSED, op_code, uncompressed_size, COMPRESSOR_SNAPPY, compressed_msg)



def parse_header(data, offset=0):
    request_id, response_to, op_code = struct.unpack_from("<III", data, offset)
    offset += 12

    if op_code == OP_COMPRESSED:
        op_code, uncompressed_size, compressor_id = struct.unpack_from(
            "<IIB", data, offset)
        offset += 9

        if compressor_id == COMPRESSOR_ZLIB:
            data = zlib.decompress(memoryview(
                data)[offset:], bufsize=uncompressed_size)
            offset = 0
        elif compressor_id == COMPRESSOR_SNAPPY and SNAPPY_SUPPORTED:
            data = snappy.uncompress(memoryview(data)[offset:])
            offset = 0
        elif compressor_id == COMPRESSOR_NOOP:
            pass
        else:
            raise ValueError("Unsupported compressor")

    return request_id, response_to, op_code, data, offset


def parse_op_msg(data, offset=0):
    uchar_struct = struct.Struct("<B")
    uint_struct = struct.Struct("<I")
    base = offset
    endpoint = len(data)
    flag_bits = uint_struct.unpack_from(data, base)[0]
    if flag_bits & 0x01:  # Checksum present
        endpoint -= 4
    base += 4
    header = None
    sections = {}
    while base < endpoint:
        payload_type = uchar_struct.unpack_from(data, base)[0]
        base += 1
        if payload_type == 0:
            base, header = bson.decode_document(data, base)
        elif payload_type == 1:
            section_end = uint_struct.unpack_from(data, base)[0] + base
            base += 4
            cstring_len = data[base:section_end].index(b"\0")
            if cstring_len == -1:
                raise ValueError("Cstring delimeter not found")
            section_header = struct.unpack_from(
                f"{cstring_len}s", data, base)[0]
            base += cstring_len + 1
            section_docs = sections.setdefault(section_header, [])

            while base < section_end:
                base, doc = bson.decode_document(data, base)
                section_docs.append(doc)

    return flag_bits, header, sections


def parse_op_reply(data ,offset=0):
    flags, cursor_id, starting_from, num_returned = struct.unpack_from("<iqii", data, offset)
    offset += 20
    docs = []
    for _ in range(num_returned):
        offset, doc = bson.decode_document(data, offset)
        docs.append(doc)
    return flags, cursor_id, starting_from, docs


def create_op_msg(header: Dict[str, Any],
                  request_id: int=0,
                  response_to: int=0,
                  more_to_come=False,
                  exhaust_allowed=False,
                  **kwargs):
    flag_bits = 0x00
    if more_to_come:
        flag_bits += 0x01
    if exhaust_allowed:
        flag_bits += 0x1_0000
    flag_bits = struct.pack("<I", flag_bits)
    header = bson.dumps(header)

    data_fragments = [flag_bits, b"0x00", header]
    total_len = 4 + 1 + len(header)

    seq_header_struct = struct.Struct("<Bi")
    for name, docs in kwargs.items():
        data = [name.encode("ascii") + b"\0"] + [bson.dumps(doc)
                                                 for doc in docs]
        section_len = sum(len(d) for d in data) + 4
        seq_header = seq_header_struct.pack(0x01, section_len)
        data_fragments.extend([seq_header] + data)
        total_len += section_len + 1

    total_len += 16
    data_fragments = [struct.pack("<iiii", total_len, request_id, response_to, OP_MSG)] + data_fragments

    return b"".join(data_fragments)


def create_op_query(
    collname: bytes,
    query: Dict[str, Any],
    request_id: int=0,
    response_to: int=0,
    tailable: bool=False,
    slaveok: bool=False,
    no_cursor_timeout: bool=False,
    await_data: bool=False,
    exhaust: bool=False,
    partial: bool=False,
    number_to_skip: int=0,
    number_to_return: int=0,
    return_field_selector: Optional[Dict[str, Any]]=None,
    compressor: Callable[[int, int, int, bytes], bytes]=none_compress):
    flags = 0
    if tailable:
        flags += 0x02
    if slaveok:
        flags += 0x04
    if no_cursor_timeout:
        flags += 0x10
    if await_data:
        flags += 0x20
    if exhaust:
        flags += 0x40
    if partial:
        flags += 0x80
    collname_len = len(collname) + 1
    header_bytes = struct.pack(f"<I{collname_len}sII", flags, collname + b"\0", number_to_skip, number_to_return)
    query_bytes = bson.dumps(query)
    data_fragments = [header_bytes, query_bytes]
    if return_field_selector is not None:
        rfs_bytes = bson.dumps(return_field_selector)
        data_fragments.append(rfs_bytes)

    return compressor(OP_QUERY, request_id, response_to, data_fragments)

def create_handshake(driver_name: str, driver_version: str, app_name: Optional[str]=None):
    document = {
        "isMaster": 1,
        "driver": {
            "name": driver_name,
            "version": driver_version
        },
        "os": {
            "type": platform.system(),
            "architecture": platform.machine(),
            "version": platform.version()
        }
    }

    compression = ["zlib"]
    if SNAPPY_SUPPORTED:
        compression.insert(0, "snappy")

    document["compression"] = compression

    request_id = next(REQUEST_ID_COUNTER)


    return create_op_query(b"admin.$cmd", document, request_id, number_to_return=1), request_id