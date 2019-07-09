import struct
import zlib
import bson
from typing import Dict, Any, List

try:
    import snappy
    SNAPPY_SUPPORTED = True
except ImportError:
    SNAPPY_SUPPORTED = False

OP_COMPRESSED = 2012
OP_MSG = 2013

COMPRESSOR_NOOP = 0
COMPRESSOR_SNAPPY = 1
COMPRESSOR_ZLIB = 2
COMPRESSOR_ZSTD = 3


class ReceiveBuffer():
    """ Buffers a raw datastream and splits into packets
    
    :ivar complete_packets: List of completed packets.
    """

    def __init__(self):
        self._message_size_bytes = bytearray(4)
        self._buffer = None
        self._write_ptr = 0
        self.complete_packets = []

    def feed(self, data: bytes):
        """ Feed data into the buffer"""
        dlen = len(data)

        if self._buffer is None:
            remaining = 4 - self._write_ptr
            if dlen >= remaining:
                self._message_size_bytes[self._write_ptr:] = data[0:remaining]
                self._create_data_buffer()
                if dlen > remaining:
                    self.feed(data[remaining:])
            else:
                self._message_size_bytes[self._write_ptr:self._write_ptr+dlen] = data
                self._write_ptr += dlen
        else:
            remaining = len(self._buffer) - self._write_ptr
            if dlen < remaining:
                self._buffer[self._write_ptr:self._write_ptr+dlen] = data
                self._write_ptr += dlen
            elif dlen >= remaining:
                self._buffer[self._write_ptr:] = data[0:remaining]
                self._finish_buffer()
                if dlen > remaining:
                    self.feed(data[remaining:])

    def _create_data_buffer(self):
        message_size = int.from_bytes(self._message_size_bytes, byteorder="little", signed=False)
        self._buffer = bytearray(message_size)
        self._buffer[0:4] = self._message_size_bytes
        self._write_ptr = 4

    def _finish_buffer(self):
        self.complete_packets.append(self._buffer)
        self._buffer = None
        self._write_ptr = 0


def parse_header(data, offset=0):
    _, request_id, response_to, op_code = struct.unpack_from("<IIII", data, offset)
    offset += 16

    if op_code == OP_COMPRESSED:
        op_code, uncompressed_size, compressor_id = struct.unpack_from("<IIB", data, offset)
        offset += 9


        if compressor_id == 2:
            data = zlib.decompress(memoryview(data)[offset:], bufsize=uncompressed_size)
            offset = 0
        elif compressor_id == 1 and SNAPPY_SUPPORTED:
            data = snappy.uncompress(memoryview(data)[offset:])
            offset = 0
        elif compressor_id == 0:
            pass
        else:
            raise ValueError("Unsupported compressor")

    return request_id, response_to, op_code, data, offset


def parse_op_msg(data):
    uchar_struct = struct.Struct("<B")
    uint_struct = struct.Struct("<I")
    base = 0
    endpoint = len(data)
    flag_bits = uint_struct.unpack_from(data, base)[0]
    if flag_bits & 0x01: #Checksum present
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
            section_header = struct.unpack_from(f"{cstring_len}s", data, base)[0]
            base += cstring_len+1
            section_docs = sections.setdefault(section_header, [])

            while base < section_end:
                base, doc = bson.decode_document(data, base)
                section_docs.append(doc)
            
    return flag_bits, header, sections

def create_op_msg(header: Dict[str, Any], more_to_come=False, exhaust_allowed=False, **kwargs):
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
        data = [name.encode("ascii") + b"\0"] + [bson.dumps(doc) for doc in docs]
        section_len = sum(len(d) for d in data) + 4
        seq_header = seq_header_struct.pack(0x01, section_len)
        data_fragments.extend([seq_header] + data)
        total_len += section_len + 1

    return data_fragments, total_len
