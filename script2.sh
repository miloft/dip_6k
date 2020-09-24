#!/usr/local/bin/bash
python3 ./versions.py $2
mysql < "add_proc_file.sql"
