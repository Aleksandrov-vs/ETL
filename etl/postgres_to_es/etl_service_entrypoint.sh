#!/bin/sh
python es_create_index.py
python main.py
exec "$@"