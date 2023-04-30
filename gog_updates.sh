#!/bin/bash

cd scripts

python3 gog_forums_scan.py

python3 gog_products_scan.py -n
python3 gog_products_scan.py -u
#uncomment if you also want to recheck delisted ids
#python3 gog_products_scan.py -d
python3 gog_products_scan.py -e

python3 gog_builds_scan.py -u
#uncomment if you also want to recheck removed builds
#python3 gog_builds_scan.py -r
#uncomment if you want to track differences between
#offline installers and Galaxy builds (delta scan)
#python3 gog_builds_scan.py -d

python3 gog_prices_scan.py -u

python3 gog_ratings_scan.py -u

#uncomment if you want to generate statistical charts
#python3 gog_plot_gen.py -t
#python3 gog_plot_gen.py -d
#only relevant if you set the correct CUTOFF_DATE
#python3 gog_plot_gen.py -i

cd ..

