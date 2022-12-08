import multiprocessing
from pathlib import Path
from urllib.parse import urlparse

import requests

FILE_URL = urlparse('http://127.0.0.1:5000/static/test.zip')
FILE_PATH = Path(FILE_URL.path)

OUTPUT_PATH = Path(__file__).parent

def main():
    s = requests.get(FILE_URL.geturl())

    with open(OUTPUT_PATH/FILE_PATH.name, 'wb') as f:
        f.write(s.content)

if __name__ == '__main__':
    main()