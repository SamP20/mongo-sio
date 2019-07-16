import pytest
import mongo_sio
from mongo_sio import parse_header, parse_op_msg, parse_op_reply, OP_COMPRESSED, OP_MSG, COMPRESSOR_ZLIB, COMPRESSOR_NOOP, COMPRESSOR_SNAPPY
import zlib
import struct
import bson


samples = [
    bytes([8, 0, 0, 0, 1, 2, 3, 4]),
    bytes([9, 0, 0, 0, 5, 6, 7, 8, 9]),
    bytes([12, 0, 0, 0, 10, 11, 12, 13, 14, 15, 16, 17]),
    bytes([6, 0, 0, 0, 18, 19])
]


@pytest.mark.parametrize("data",[range(256), 128])
@pytest.mark.parametrize("compressor", mongo_sio.SUPPORTED_COMPRESSORS)
def test_compressed_header_zlib(data, compressor):
    body = bytes(data)

    if compressor == COMPRESSOR_ZLIB:
        body_compressed = zlib.compress(body)
    elif compressor == COMPRESSOR_SNAPPY:
        # pylint: disable=import-error
        import snappy
        body_compressed = snappy.compress(body)
    elif compressor == COMPRESSOR_NOOP:
        body_compressed = body
    
    packed = struct.pack(f"<IIIIIB{len(body_compressed)}s",
        345,
        567,
        OP_COMPRESSED,
        OP_MSG,
        len(body),
        compressor,
        body_compressed
    )

    request_id, response_to, op_code, parsed_data, offset = parse_header(packed)
    assert request_id == 345
    assert response_to == 567
    assert op_code == OP_MSG
    assert parsed_data[offset:] == body

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


def test_parse_op_reply():
    document = {
        "A": "b",
        "B": 42,
        "C": {"hello": False}
    }

    flags = 0x55aaff00
    cursor_id = 0x7799aabb_ccddeeff
    starting_from = 0x11223344
    num_returned = 1
    doc_bytes = bson.dumps(document)
    data = struct.pack(f"<iqii{len(doc_bytes)}s", flags, cursor_id, starting_from, num_returned, doc_bytes)

    out_flags, out_cursor_id, out_starting_from, out_docs = parse_op_reply(data)
    assert out_flags == flags
    assert out_cursor_id == cursor_id
    assert out_starting_from == starting_from
    assert len(out_docs) == 1
    assert out_docs[0] == document
