import aiohttp
import async_timeout
import os
from pathlib import Path
import gzip
import logging

logging.basicConfig(level=logging.INFO)


class extract():
    def __init__(self, year, month, day, in_path, out_path):
        self.year = year
        self.month = month
        self.day = day
        self.in_path = os.path.join(in_path, str(self.year), str(self.month), str(self.day))
        self.out_path = os.path.join(out_path, str(self.year), str(self.month), str(self.day))
        self.__create_raw_bucket()

        print(self.in_path)

    def __create_raw_bucket(self):
        Path(self.in_path).mkdir(parents=True, exist_ok=True)
        Path(self.out_path).mkdir(parents=True, exist_ok=True)

        return None

    def __get_url(self):
        day = f'{self.day:02d}'
        hours = list(range(0, 24))
        urls = [f'https://data.gharchive.org/{self.year}-{self.month:02d}-{day}-{hour}.json.gz' for hour in hours]
        return urls

    def __ungzip(self, in_file, out_file):
        if in_file[-3:] == ".gz":
            input = gzip.GzipFile(in_file, 'rb')
            s = input.read()
            input.close()

            output = open(out_file, 'wb')
            output.write(s)
            output.close()

    async def __extract_coroutine(self, session, url):
        try:
            with async_timeout.timeout(10):
                async with session.get(url) as response:
                    filename = os.path.join(self.in_path, os.path.basename(url))
                    logging.info("FETCHING: %s", filename)
                    with open(filename, 'wb') as f_handle:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            f_handle.write(chunk)

                    out_file = os.path.join(self.out_path, os.path.basename(url))[:-3]
                    logging.info("UNPACKING: %s", out_file)
                    self.__ungzip(filename, out_file)
                    return await response.release()
        except Exception as e:
            print(e)

    async def main(self, loop):
        urls = self.__get_url()[:5]
        async with aiohttp.ClientSession(loop=loop) as session:
            for url in urls:
                await self.__extract_coroutine(session, url)
