import multiprocessing
import asyncio
import aiohttp
from pathlib import Path
from urllib.parse import urlparse

import requests

FILE_URL = urlparse('http://127.0.0.1:5000/static/test.zip')
FILE_PATH = Path(FILE_URL.path)

OUTPUT_PATH = Path(__file__).parent

SPLIT_NUM = 10


async def main():
    content = await parallel_download(FILE_URL.geturl())

    with open(OUTPUT_PATH/FILE_PATH.name, 'wb') as f:
        f.write(content)


async def parallel_download(url):
    file_length = int(requests.head(url).headers['Content-Length'])
    chunk_size = file_length//SPLIT_NUM

    content = b''
    downloads = []

    for start in range(0, file_length, chunk_size):
        downloads.append(partial_download(url, start, chunk_size))
    content = await asyncio.gather(*downloads)
    content = b''.join(content)

    return content


async def partial_download(url, start_byte, chunk_size):
    headers = {'Range': f'bytes={start_byte}-{start_byte+chunk_size-1}'}

    print(f'starting download {headers["Range"]}')
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            content = await resp.read()
            print(f'finished download {headers["Range"]}')
            return content

if __name__ == '__main__':
    asyncio.run(main())
