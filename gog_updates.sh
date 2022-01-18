#!/bin/bash

cd scripts

python3 gog_products_scan.py -n
python3 gog_products_scan.py -t
python3 gog_products_scan.py -u
python3 gog_products_scan.py -e

python3 gog_prices_scan.py -f

cd ..
