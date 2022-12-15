import multiprocessing
from pathlib import Path
from urllib.parse import urlparse

import requests

FILE_URL = urlparse('http://127.0.0.1:5000/static/test.zip')
FILE_PATH = Path(FILE_URL.path)

OUTPUT_PATH = Path(__file__).parent

SPLIT_NUM = 10

def main():
    content = parallel_download(FILE_URL.geturl())

    with open(OUTPUT_PATH/FILE_PATH.name, 'wb') as f:
        f.write(content)

def parallel_download(url):
    file_length = int(requests.head(url).headers['Content-Length'])
    print(file_length)
    chunk_size = file_length//SPLIT_NUM
    print(chunk_size)

    content = b''

    for start in range(0, file_length, chunk_size):
        # Change this part to implement parallelization
        partial_content = partial_download(url, start, chunk_size)
        content += partial_content
    
    return content

def partial_download(url, start_byte, chunk_size):
    headers = {'Range': f'bytes={start_byte}-{start_byte+chunk_size-1}'}

    print(headers['Range'])
    stream = requests.get(FILE_URL.geturl(), headers=headers)

    return stream.content

if __name__ == '__main__':
    main()