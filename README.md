# gog_visor
A collection of scripts that call publicly available GOG APIs in order to keep track of game, installer and pricing updates.

## What does Visor do?

It provides several python3 scripts which call publicly available GOG APIs in order to collect product data and store it in an SQLite database, which can then be queried in order to detect updates and other changes to developer/publisher entries, game cards, changelogs, installers/files and prices.

## What do I need to do to get it running on my PC?

**1.** You will need a **python3.6+** environment. Most Linux distros will come with python3 installed - make sure you pick one which comes with **python 3.6** or above.

**2.** The following python3 packages need to be installed: `html2text, requests, lxml, matplotlib, tk`

On Linux, you will need to install the packages using the distro's default package manager. For Debian-based/derived distros, this should do the trick:
```
sudo apt-get install python3-html2text python3-requests python3-lxml python3-matplotlib python3-tk
```

**3.** Switch to the scripts directory:
```
cd scripts
```

**4.** Create the Visor SQLite database, along with the appropriate tables and other artifacts, by running:
```
python3 gog_create_db.py
```

The database will be created in the *output_db* folder.

**5.** Collect and save developer/publisher data from the GOG website, by running the *gog_company_scan.py* script:
```
python3 gog_company_scan.py -f
```

**6.** Do a manual scan to populate the Visor database with the first 10 ids (to skip the gap between id 10 and the next populated id at ~1070000000):
```
python3 gog_products_scan.py -m
```

**7.** Do an initial full scan to populate the Visor database with current game ids (note that this will scan the entire GOG id range from 1070000000 to 2147483647 and may take about a week to complete):
```
python3 gog_products_scan.py -f
```

The scan can be stopped at any point in time and will resume from where it left off once you run it again. You can, in theory, increase the thread count in the *gog_products_scan.conf* file to speed things up, but you risk getting throttled or even getting your IP temporarily banned by GOG. Sticking with the defaults is recommended.


**8.** Populate initial installer & patch data (*installer/file table*) - this info will be extracted from the data previously collected during the full game id scan:
```
python3 gog_products_scan.py -e
```

**9.** Populate initial pricing data, based on the previously collected product ids:

**IMPORTANT** First make sure you've set the currency list/region you're interested in tracking by editing the *gog_prices_scan.conf* file. See the below section on pricing scans for more details.
```
python3 gog_prices_scan.py -f
```

You're now good to go!

All 4 database tables should be populated with data. This is essentially a snapshot of all the developer/publisher names, game ids, associated installer/file entries and (optionally) prices reported by the GOG APIs.

## How do I handle update scans?

Assuming you've followed the steps described above, you are now ready to do delta runs and detect any new developer/publisher entries along with changes to game ids, associated installer/file entries and prices.

All you need to do is run the provided update bash script:
```
./gog_updates.sh
```

Wait for the script to finish collecting all the required data.

You can now run the provided SQL queries against the *gog_visor.db* file to list the updates. The queries are included in the sql script file (*sql\gog_updates.sql*). Any SQLite client can be used to this purpose. I personally recommend getting **DB Browser for SQLite**: https://sqlitebrowser.org/.

## What about pricing scans?

The *gog_prices_scan.py* script will retrieve pricing data for configured currencies (or *all* currencies, as reported by the API) in a certain region. Multiple regions may be kept track of, but would require separate scans with the *country_code* parameter set accordingly in the config file.

Pricing scans will automatically be triggered by update scans, as described above - price changes will be logged as new entries while exiting ones will be outdated, in the interest of tracking historical data and trends. I've also included an sql script for listing discounts based on collected data.

## Disclaimer

I'm sure some may disagree with my style of coding or the lack of OOPness in my code. That's fair enough. Just FYI, I did not write these scripts with the intention of sharing them with anyone in particular (at least not initially), so you'll be bearing the full brunt of what I deemed was the most easily maintainable and hackable code I could write. Feel free to improve it as you see fit.
