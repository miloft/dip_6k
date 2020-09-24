from datetime import datetime
import aiohttp
import aiofiles
import asyncio
import re
import math
import os


class File(object):
    def __init__(self, file_name, file_type, file_size, file_update, orbit_id, orbit_update):
        self.name = file_name
        self.type = file_type
        self.size = file_size
        self.dt_update = file_update
        self.orbit_id = orbit_id
        self.orbit_update = orbit_update
        self.instrument_id = 1


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    power = math.pow(1024, i)
    size = round(size_bytes / power, 2)
    return f"{size} {size_name[i]}"


async def fetch(client, url):
    async with client.get(url) as resp:
        return await resp.text()


async def check():
    async with aiohttp.ClientSession() as client:
        html_main = await fetch(client, url_main)
        re_orbits = re.findall(r'(\d{1,2}/\d{1,2}/\d{4}).*?\>(.*?)\<', html_main)  # (date,name)
        orbits = [list(tuple) for tuple in re_orbits]  # [date,name]
        for elem in orbits:
            elem[0] = datetime.strptime(elem[0], '%m/%d/%Y').date()
            if elem[0] > last_update:
                update_orbits.append({'orbit_update': elem[0], 'orbit_id': elem[1]})
        logs.write(f"READ FOLDERS: {len(orbits)}\n")
        logs.write(f"NEW FOLDERS: {len(update_orbits)}\n")


async def open_folders(folder):
    sem = asyncio.Semaphore(100)
    async with sem, aiohttp.ClientSession() as client:
        folder_url = url_main + folder['orbit_id'] + '/'
        html = await fetch(client, folder_url)
        output_files = re.findall(r'(\d{1,2}/\d{1,2}/\d{4}).*?M\s*(\d*).*?>(\w+.\w+)\<', html)  # date+size+name
        for elem in output_files:
            file = File(file_name=elem[2],
                        file_type=elem[2][-3:],
                        file_size=elem[1],
                        file_update=datetime.strptime(elem[0], '%m/%d/%Y').date(),
                        orbit_id=folder['orbit_id'],
                        orbit_update=folder['orbit_update'])
            data.append(file)
            size_bytes.append(int(file.size))


async def download(obj):
    sem = asyncio.Semaphore(1000)
    async with sem, aiohttp.ClientSession() as client:
        url = url_main + obj.orbit_id + '/' + obj.name

        path = folderpath + obj.name  # for Unix
        async with client.get(url, timeout=None) as resp:
            file = await resp.read()
        async with aiofiles.open(path, mode='wb') as out:
            await out.write(file)
            logs.write(f"{datetime.now()} {obj.name}\n")
            add_to_db.write(f"{obj.type}|{obj.dt_update}|{obj.orbit_id}|{obj.orbit_update}|"
                            f"{obj.instrument_id}|{obj.name}|{obj.size}|NULL|NULL\n")


def partition():
    for i in range(len(data) // download_cycle):
        part_tasks.append([download(data[j]) for j in range(download_cycle * i, download_cycle * (i+1))])

    if len(data) % download_cycle:
        new_data = data[-1 * (len(data) % download_cycle):]
        part_tasks.append([download(obj) for obj in new_data])


# directory of MEX-data
url_main = 'https://pds-geosciences.wustl.edu/mex/mex-m-hrsc-5-refdr-mapprojected-v3/mexhrs_1003/data/'
folderpath = '/data/raw_data/'  # for server
if not os.path.exists(folderpath):
    os.makedirs(folderpath)

with open('info.log', 'rb') as logs:
    logs.seek(-12, 2)  # seek EOF
    last_update = datetime.strptime(logs.read(10).decode(), '%Y-%m-%d').date()

with open('info.log', 'ta+') as logs, open('add_file.sql', 'w') as add_to_db:
    logs.write(f"START: {datetime.now()}\n")

    loop = asyncio.get_event_loop()
    update_orbits = []
    loop.run_until_complete(check())
    data = []
    size_bytes = []
    if len(update_orbits):
        tasks = [open_folders(folder) for folder in update_orbits]
        loop.run_until_complete(asyncio.gather(*tasks))
        if len(data):  
            logs.write(f"NEW FILES: {len(data)}\n")
            logs.write(f"SIZE OF FILES: {convert_size(sum(size_bytes))}\n")
            part_tasks = []
            download_cycle = 200
            partition()

            for i in range(len(part_tasks)):
                loop.run_until_complete(asyncio.gather(*part_tasks[i]))

    loop.close()

    logs.write(f"END: {datetime.now()}\n")
    logs.write(f"LAST UPDATE: {datetime.now().date()}\n")
