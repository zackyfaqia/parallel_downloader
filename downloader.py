import sys
import asyncio
import tempfile
import argparse
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import aiomultiprocess

OUTPUT_PATH = Path(__file__).parent # Directory for saved downloads

parser = argparse.ArgumentParser(
    description='''\
        Parallel Downloader.
        Download files from multiple urls in parallel.\
        '''
)
parser.add_argument('urls', type=str, nargs='+', help='Url from where the file can be downloaded')
parser.add_argument('-s', '--stream',type=int, default=10, help='number of stream to be generated per url', metavar='STREAM_COUNT')
parser.add_argument('-b', '--buffer', type=int, default=1024*5, help='buffer size per stream', metavar='BUFFER_SIZE')
args = parser.parse_args()


NUM_OF_SESSION = args.stream     # To create a number of connection session. Each session download the total file size divided by the number of session.
BUFFER_SIZE = args.buffer    # How much data to be stored in memory before appended to temp file


async def main(*args):
    '''
        Run parallel download
    '''
    await parallel_download(args)


async def parallel_download(urls):
    '''
        Download multiple files from urls simultaneously.
        Return filepaths of downloaded files as a list.
    '''
    args = []
    for url in urls:
        file_url = urlparse(url)
        args.append((file_url.geturl(), OUTPUT_PATH/Path(file_url.path).name))
    async with aiomultiprocess.Pool() as pool:
        saved_paths = await pool.starmap(concurrent_download, args)

    return saved_paths

async def concurrent_download(url, save_path):
    '''
        Download a file asynchronously by dividing the file to multiple part and creating multiple stream for each part.
        Return filepath of downloaded file as a string.
    '''
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as resp:
                file_length = int(resp.headers['Content-Length'])
    except aiohttp.ClientConnectionError:
        print(f"Couldn't connect to {url}")
        return

    chunk_size = file_length//(NUM_OF_SESSION-1)

    downloads = []

    for part_num, start in enumerate(range(0, file_length, chunk_size), 1):
        downloads.append(_partial_download(url, start, chunk_size, part_num))
    content = await asyncio.gather(*downloads)

    with open(save_path, 'wb') as f:
        for part in content:
            with open(part, 'rb') as p:
                f.write(p.read())
        print(f'({url}) saved download')

    return save_path


async def _partial_download(url, start_byte, chunk_size, part_num):
    '''
        Download part of a file.
        Return filepath of downloaded part as a string.
    '''
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
    asyncio.run(main(*args.urls))
