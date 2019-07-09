import pytest
import mongo_sio.wire
from mongo_sio.wire import ReceiveBuffer, parse_header, parse_op_msg, OP_COMPRESSED, OP_MSG, COMPRESSOR_ZLIB, COMPRESSOR_NOOP
import zlib
import struct
import bson

samples = [
    bytes([8, 0, 0, 0, 1, 2, 3, 4]),
    bytes([9, 0, 0, 0, 5, 6, 7, 8, 9]),
    bytes([12, 0, 0, 0, 10, 11, 12, 13, 14, 15, 16, 17]),
    bytes([6, 0, 0, 0, 18, 19])
]


def test_exact_size_buffers():
    rbuf = ReceiveBuffer()
    for i, s in enumerate(samples):
        rbuf.feed(s)
        assert rbuf.complete_packets == samples[:i+1]


@pytest.mark.parametrize("split",[2, 3, 4, 5])
def test_split_buffer(split):
    rbuf = ReceiveBuffer()
    for i, s in enumerate(samples):
        rbuf.feed(s[:split])
        rbuf.feed(s[split:])
        assert rbuf.complete_packets == samples[:i+1]


def test_merged_buffers():
    rbuf = ReceiveBuffer()
    data = b"".join(samples)
    rbuf.feed(data)
    assert rbuf.complete_packets == samples

@pytest.mark.parametrize("data",[range(256), 128])
@pytest.mark.parametrize("compressor",[COMPRESSOR_ZLIB, COMPRESSOR_NOOP])
def test_compressed_header_zlib(data, compressor):
    body = bytes(data)

    if compressor == COMPRESSOR_ZLIB:
        body_compressed = zlib.compress(body)
    elif compressor == COMPRESSOR_NOOP:
        body_compressed = body
    
    total_len = 4+4+4+4+4+8+len(body_compressed)
    packed = struct.pack(f"<IIIIIIB{len(body_compressed)}s",
        total_len,
        345,
        567,
        OP_COMPRESSED,
        OP_MSG,
        len(body),
        compressor,
        body_compressed
    )

    request_id, response_to, op_code, parsed_data = parse_header(packed)
    assert request_id == 345
    assert response_to == 567
    assert op_code == OP_MSG
    assert parsed_data == body

def test_parse_op_msg():
    header_doc = {
        "insert": "test",
        "$db": "mydb",
        "writeConcern": {"w": "majority" }
    }
    header_bytes = bson.dumps(header_doc)

    body0_ident = b"documents\x00"
    body0_doc = {"_id": "Document#1", "myvar": 42}
    body0_bytes = bson.dumps(body0_doc)
    flagbits = 0x02.to_bytes(4, byteorder="little", signed=False) #MoreToCome bit

    bodysize = (len(body0_ident) + len(body0_bytes) + 4).to_bytes(4, byteorder="little", signed=False)

    packet = flagbits + b"\x00" + header_bytes + b"\x01" + bodysize + body0_ident + body0_bytes
    
    out_flagbits, out_header_doc, out_sections = parse_op_msg(packet)

    assert header_doc == out_header_doc
    assert {b"documents": [body0_doc]} == out_sections
    assert out_flagbits == 0x02
