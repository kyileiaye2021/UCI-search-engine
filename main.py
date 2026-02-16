import json
from posting import Posting
import os
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from collections import Counter, defaultdict
import re

INDEX = defaultdict(list) # create an inverted index to store the tokens and a list of postings 

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

def build_postings(doc_id, all_tokens, important_tokens):
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
        
        INDEX[token].append(posting)

def read_json():
    """
    Read the json files from DEV folder/sub folders
    
    Args: None
    Return: None
    """
    ROOT_DIR = "DEV"
    DOC_ID = 0
    
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

                        build_postings(DOC_ID, all_tokens, important_tokens)
                        
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error reading {file_path}: {e}")
                    
def main():
    read_json()
    print("Index: ")
    print(INDEX)
    
    
if __name__ == "__main__":
    main()