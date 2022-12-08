import multiprocessing
from pathlib import Path
from urllib.parse import urlparse

import requests

FILE_URL = urlparse('http://127.0.0.1:5000/static/test.zip')
FILE_PATH = Path(FILE_URL.path)

OUTPUT_PATH = Path(__file__).parent

SPLIT_NUM = 10

def main():
    file_length = int(requests.head(FILE_URL.geturl()).headers['Content-Length'])
    print(file_length)

    chunk_size = file_length//SPLIT_NUM

    content = b''
    
    for start in range(0, file_length, chunk_size):
        headers = {'Range': f'bytes={start}-{start+chunk_size-1}'}

        print(headers['Range'])
        s = requests.get(FILE_URL.geturl(), headers=headers)
        content += s.content

    with open(OUTPUT_PATH/FILE_PATH.name, 'wb') as f:
        f.write(content)

if __name__ == '__main__':
    main()