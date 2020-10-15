#!/bin/bash

cd scripts

python3 gog_company_scan.py -f

if [ $? -eq 0 ]
then
    python3 gog_products_scan.py -n
    python3 gog_products_scan.py -t
    python3 gog_products_scan.py -u
    
    if [ $? -eq 0 ]
    then
        python3 gog_products_scan.py -e
        
        if [ $? -eq 0 ]
        then
            python3 gog_prices_scan.py -f
        fi
    fi
fi

cd ..
