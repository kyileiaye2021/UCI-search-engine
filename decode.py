import struct
def decode(data):
    """
    Decoding the raw binary to text

    Args:
        data (Posting): Posting (doc id, term frequency, important)
    
    Return - list(Posting):
        a list of postings 
    """
    postings = []
    size = struct.calcsize("IIB")
    
    for i in range(0, len(data), size):
        doc_id, tf, important = struct.unpack(
            "IIB", data[i : i + size]
        )
        postings.append((doc_id, tf, important))
    return postings