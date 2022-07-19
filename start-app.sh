#!/usr/bin/env bash

python manage.py waitdb 
python manage.py migrate 
cd /opt/app/ 
pwd 
./start-server.sh
