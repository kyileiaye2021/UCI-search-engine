import pickle
from indexer import preprocess_text
from decode import decode

BYTE_POSITION_OFFSET_FILE = "byte_position.pkl" 
MAPPING_FILE = "doc_mapping.pkl"
MERGED_INDEX = "merged_index.bin"

def load_byte_pos_offset_file():
    """
    Loading byte position offset data file to use for locating the byte position of postings
    
    Returns:
        a dictionary of term and its corresponding byte position and len of postings
    """
    with open(BYTE_POSITION_OFFSET_FILE, 'rb') as f:
        posting_byte_position = pickle.load(f)
        return posting_byte_position

def load_doc_mapping_file():
    """
    Loading load doc mapping data file to use for retrieving urls based on retrieved doc ID
    
    Returns:
        a dictionary of doc id and its corresponding url
    """
    with open(MAPPING_FILE, 'rb') as f:
        doc_url_mapping = pickle.load(f)
        return doc_url_mapping
    
def preprocess_query(search_query):
    """
    Apply the same rule of preprocessing search query as the documents

    Args:
        search_query (str): user search query

    Returns:
        a list of tokenized and stemmed tokens
    """
    tokens = preprocess_text(search_query)
    return tokens
    
def read_postings(token, posting_byte_pos):
    """
    Look up the term in posting byte pos and retrieve the posting docs in O(1) time

    Args:
        token(str): a token or term from a list of user query tokens
        posting_byte_pos (dict(tuple)): a dictionary of term and its corresponding byte position and len of postings

    Returns:
        list(Postings): a list of postings
    """
    if token not in posting_byte_pos:
        return []
    
    offset, length = posting_byte_pos[token]
    with open(MERGED_INDEX, "rb") as f:
        f.seek(offset)
        byte_raw_data = f.read(length)
        posting_data = decode(byte_raw_data)
        
    return posting_data

def intersect(p1, p2):
    """
    Applying boolean AND to retrieve the docs that have both terms

    Args:
        p1 (list(Postings)): a posting of first term
        p2 (list(Postings)): a posting of second term

    Returns:
        _type_: _description_
    """
    i = j = 0
    result = []
    while i < len(p1) and j < len(p2):
        if p1[i][0] == p2[j][0]:
            result.append(p1[0])
            i += 1
            j += 1
            
        elif p1[i][0] < p2[j][0]:
            i += 1
        
        else:
            j += 1
    return result

def search_query(query_tokens, posting_byte_pos):
    posting_list = []
    for term in query_tokens:
        read_postings(term)
        
def main():
    query = input("Enter your query: ")
    query_tokens = preprocess_query(query)