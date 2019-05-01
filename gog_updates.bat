@ECHO OFF
cd scripts

python gog_company_scan.py -f

if not errorlevel 1 (
    python gog_products_scan.py -n
    python gog_products_scan.py -t
    python gog_products_scan.py -u
    
    if not errorlevel 1 (
        python gog_products_scan.py -e
    )
)

cd ..
