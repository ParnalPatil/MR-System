#!/bin/bash
nohup python3 database/ser_db.py $1 </dev/null 2>&1 >>database_log &