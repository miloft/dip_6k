#!/usr/local/bin/bash
python3 /home/shadrina/download.py
/home/shadrina/proc/fdb/add -mode ignore mars_raw_data data /data/raw_data < "add_file.sql" > "id_file.txt"
