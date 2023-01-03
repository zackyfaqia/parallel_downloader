import aiomultiprocess
import sys
import asyncio
import aiohttp
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import requests

FILE_URL = urlparse('http://127.0.0.1:5000/static/test.zip')
FILE_PATH = Path(FILE_URL.path)

OUTPUT_PATH = Path(__file__).parent

SPLIT_NUM = 10
BUFFER_SIZE = 1024*5


async def main(*args):
    await parallel_download(args)


async def parallel_download(urls):
    args = []
    for url in urls:
        file_url = urlparse(url)
        args.append((file_url.geturl(), OUTPUT_PATH/Path(file_url.path).name))
    async with aiomultiprocess.Pool() as pool:
        await pool.starmap(concurrent_download, args)

async def concurrent_download(url, save_path):
    # try:
    #     file_length = int(requests.head(url).headers['Content-Length'])
    # except requests.exceptions.ConnectionError:
    #     print(f'Connection to {url} refused')
    #     return

    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as resp:
                file_length = int(resp.headers['Content-Length'])
    except aiohttp.ClientConnectionError:
        print(f"Couldn't connect to {url}")
        return

    chunk_size = file_length//(SPLIT_NUM-1)

    content = b''
    downloads = []

    for part_num, start in enumerate(range(0, file_length, chunk_size), 1):
        downloads.append(partial_download(url, start, chunk_size, part_num))
    content = await asyncio.gather(*downloads)
    # content = b''.join(content)
    # print(content)

    with open(save_path, 'wb') as f:
        for part in content:
            with open(part, 'rb') as p:
                f.write(p.read())
                # print(f'written {part}')
        print(f'({url}) saved download')


async def partial_download(url, start_byte, chunk_size, part_num):
    headers = {'Range': f'bytes={start_byte}-{start_byte+chunk_size-1}'}
    save_path = tempfile.gettempdir() + Path(urlparse(url).path).name + f'.part{part_num}'

    print(f'({url}) starting download {headers["Range"]}')
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            # content = await resp.read()
            print(f'({url}) finished download {headers["Range"]}')
            with open(save_path, 'wb') as f:
                # print('opened')
                async for buffer in resp.content.iter_chunked(BUFFER_SIZE):
                    # print(f'buffer: {buffer}')
                    f.write(buffer)
                # print('written')
            return save_path

if __name__ == '__main__':
    asyncio.run(main(*sys.argv[1:]))
