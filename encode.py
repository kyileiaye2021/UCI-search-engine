import struct

def encode(postings):
    """_summary_

    Args:
        postings (_type_): _description_

    Returns:
        _type_: _description_
    """
    b = bytearray()
    for p in postings:
        b.extend(struct.pack("IIB", p.doc_id, p.tf, p.is_important))
    return bytes(b)