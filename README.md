# gog_visor
A collection of scripts that call publicly available GOG APIs in order to keep track of game and installer updates.

## What does Visor do?

It provides several python3 scripts which call publicly available GOG APIs in order to collect product data and store it in an SQLite database, which can then be queried in order to detect updates and other changes to developer/publisher entries, game cards, changelogs and installers/files.

## What do I need to do to get it running on my PC?

**1.** You will need a **python3.6+** environment. Most Linux distros will come with python3 installed - make sure you pick one which comes with **python 3.6** or above.

**2.** The following python3 packages need to be installed: `html2text, numpy, requests, lxml, matplotlib, tk`

On Linux, you will need to install the packages using the distro's default package manager. For Debian-based/derived distros, this should do the trick:
```
sudo apt-get install python3-html2text python3-numpy python3-requests python3-lxml python3-matplotlib python3-tk
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

**6.** Do an initial manual run to populate the Visor database with known game ids (these are listed in the */conf/gog_products_scan.conf* file, and will be updated monthly with the latest values):
```
python3 gog_products_scan.py -m
```

**7.** Populate initial installer & patch data (*installer/file table*) - this info will be extracted from the data previously collected during the manual game id scan:
```
python3 gog_products_scan.py -e
```

You're now good to go! 

All 3 database tables should be populated with data. This is essentially a snapshot of all the developer/publisher names, game ids and associated installer/file entries reported by the GOG APIs.

## How do I handle update scans?

Assuming you've followed the steps described above, you are now ready to do delta runs and detect any new developer/publisher entries along with changes to game ids and associated installer/file entries.

All you need to do is run the provided update bash script:
```
./gog_updates.sh
```

Wait for the script to finish collecting all the required data.

You can now run the provided SQL queries against the *gog_visor.db* file to list the updates. The queries are included in the sql script file (*sql\gog_updates.sql*). Any SQLite client can be used to this purpose. I personally recommend getting **DB Browser for SQLite**: https://sqlitebrowser.org/.

## Disclaimer

I'm sure some may disagree with my style of coding or the lack of OOPness in my code. That's fair enough. Just FYI, I did not write these scripts with the intention of sharing them with anyone in particular (at least not initially), so you'll be bearing the full brunt of what I deemed was the most easily maintainable and hackable code I could write. Feel free to improve it as you see fit.
