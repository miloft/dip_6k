# dip_6k

This project use
* MySQL as DataBase + FDB utilities (*not free for distribution*)
* aiohttp, asyncio, aiofiles
* NASA's Planetary Data System (PDS) archives

## Run
1. For start the program you need add file "info.log" where the last string will look like: LAST UPDATE: 2020-01-01 

2. python3 ./program.py
   This program download data, update "info.log" and create "add_file.sql"

3. You need fdb.conf file
   ./add -mode ingnore mars_raw_data /data/raw_data < "add_file.sql" > "id_file.txt"

4. Create "log_file" with the records about processing data

5. python3 ./version.py log_file
   This program create "add_proc_file.sql"

6. You need ./my.cnf file
   mysql < "add_proc_file.sql"

7. You're breathtaking!

## Disclaimer
:shit: no comments in this source code
