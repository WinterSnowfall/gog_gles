#!/bin/bash

DAYOFWEEK=$(date +"%u")
# change to your preferred day of the week
WEEKLYSCANDAY=6

cd scripts

./gog_forums_scan.py

./gog_support_scan.py

./gog_products_scan.py -n
./gog_products_scan.py -u
./gog_products_scan.py -e

./gog_builds_scan.py -u
./gog_builds_scan.py -p
./gog_builds_scan.py -d

./gog_products_scan.py -b

if [ $DAYOFWEEK = $WEEKLYSCANDAY ]
then
    ./gog_products_scan.py -c
    ./gog_products_scan.py -r
    ./gog_products_scan.py -d

    ./gog_builds_scan.py -r

    ./gog_releases_scan.py -u
    ./gog_releases_scan.py -p
    ./gog_releases_scan.py -r

    # can be moved outside of the weekly scan block
    # if more regular pricing updates are preferred
    ./gog_prices_scan.py -u
    ./gog_prices_scan.py -a

    # can be moved outside of the weekly scan block
    # if more regular ratings updates are preferred
    ./gog_ratings_scan.py -u
    ./gog_ratings_scan.py -r

    # uncomment if you want to generate statistical charts
    #./gog_plot_gen.py -t -f id
    #./gog_plot_gen.py -t -f release
    #./gog_plot_gen.py -d
    # only relevant if you set the correct cutoff_date
    #./gog_plot_gen.py -i
fi

cd ..

