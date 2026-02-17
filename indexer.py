import json
from posting import Posting
import os
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from collections import Counter, defaultdict
import pickle
import re

CHUNK_SIZE = 14000
# create an inverted index to store the tokens and a list of postings 

def preprocess_text(content):
    """
    Tokenize the page content and apply stemming
    
    Args:
        content (str): contents of the url page from json file
    Return:
        a list of tokenized and stemmed tokens
    """
    tokens = re.compile(r"[A-Za-z0-9]+").findall(content.lower())
    stemmer = PorterStemmer()
    return [stemmer.stem(t) for t in tokens]

def parse_url_content(content):
    """
    Parse HTML content
    - Extract all text
    - Distinguish important text

    Args:
        content (str): contents of the url page from json file
    Return:
    
    """
    IMPORTANT_TAGS = ["title", "h1", "h2", "h3", "b", "strong"]
    soup = BeautifulSoup(content, "html.parser")
    important_tokens = set()
    all_tokens = []
    
    # for important text
    for tag in IMPORTANT_TAGS:
        for ele in soup.find_all(tag):
            tokens = preprocess_text(ele.get_text())
            important_tokens.update(tokens)
            all_tokens.extend(tokens)
    
    # for body text
    body_text = soup.get_text(separator=" ")
    tokens = preprocess_text(body_text)
    all_tokens.extend(tokens)
    
    return all_tokens, important_tokens

def build_index(doc_id, all_tokens, important_tokens, CHUNK_INDEX):
    """
    Creating a map between all tokens to postings {token (str): posting(obj)}

    Args:
        doc_id (int): Document id
        tf (int): frequency count of the token appeared in the doc
        important_tokens (set): tokens considered as important because of the occurence in headings/titles
    """
    token_frequency = Counter(all_tokens)
    for token, tf in token_frequency.items():
        is_important = token in important_tokens
        posting = Posting(doc_id, tf, is_important)
        
        CHUNK_INDEX[token].append(posting)
 
def save_chunk(CHUNK_INDEX, CHUNK_ID):
    """
    Saving the files in chunks.
    CHUNK SIZE - 14000
    Every 14000 docs, there is one chunk saved.
    
    Args:
        CHUNK_INDEX - temporaray in-memory inverted index
        CHUNK_ID - chunk file number
    
    Return:
        None
    """
    CHUNK_DIR = "index_chunks"
    filename = os.path.join(CHUNK_DIR,f"partial_{CHUNK_ID}.pkl")
    with open(filename, "wb") as f:
        pickle.dump(CHUNK_INDEX, f)
        
def read_json():
    """
    Read the json files from DEV folder/sub folders
    Extract content from the files
    Preprocess the text and important tokens
    Build inverted index in chunks
    Save the chunks and merge
    
    Args: None
    Return: None
    """
    ROOT_DIR = "TEST_DATA"
    DOC_ID = 0
    CHUNK_ID = 0
    CHUNK_INDEX = defaultdict(list)
    
    for root, dirs, files in os.walk(ROOT_DIR):
        for file in files:
            print(file)
            
            if file.endswith(".json"):
                
                DOC_ID += 1
                file_path = os.path.join(root, file)
                print(f"Reading: {file_path}")
                
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        # url = data['url']
                        content = data['content']

                        all_tokens, important_tokens = parse_url_content(content)

                        build_index(DOC_ID, all_tokens, important_tokens, CHUNK_INDEX)
                        
                        # if the doc count hits 14000, it is stored in one chunk
                        if DOC_ID % CHUNK_SIZE == 0:
                            save_chunk(CHUNK_INDEX, CHUNK_ID)
                            CHUNK_INDEX.clear()
                            CHUNK_ID += 1
                        
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error reading {file_path}: {e}")
    # save final partial chunk
    if CHUNK_INDEX:
        save_chunk(CHUNK_INDEX, CHUNK_ID)
        
def main():
    read_json()
    
if __name__ == "__main__":
    main()