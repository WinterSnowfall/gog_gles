# gog_visor
A collection of scripts that call publicly available GOG APIs in order to keep track of game and installer updates.

## What does Visor do?

It provides several python3 scripts which query publicly available GOG APIs in order to collect company and game data and store it in an SQLite database, which can then be queried in order to detect updates and other changes to game cards, changelogs and installers/files.

## What do I need to do to get it running on my PC?

**1.** You will need a **python3.6+** environment - I recommend getting **WinPython** if you're running the scripts on windows: https://winpython.github.io/. Most Linux distros will come with python3 installed - make sure you pick one which has **version 3.6** or above.

**2.** The following python3 packages need to be installed: `html2text, numpy, requests, lxml`

On WinPython (Zero) you can get them by opening an admin console and using the following commands (numpy is already installed):
```
pip install html2text
pip install requests
pip install lxml
```

On Linux, you will need to install the right packages. For Debian-based/derived distros, this should do the trick:
```
sudo apt-get install python3-html2text python3-numpy python3-requests python3-lxml
```

**3.** Switch to the scripts directory:
```
cd scripts
```

**4.** Collect and save developer/publisher data from the GOG website, by running the gog_company_scan script:
```
python3 gog_company_scan -f
```

**5.** Do an initial manual run to populate the Visor database with known game ids (these are listed in the /conf/gog_products_scan.conf file, and will be updated monthly with the latest values):
```
python3 gog_products_scan -m
```

**6.** Populate initial installer & patch data (*installer/file table*) - this info will be extracted from the data previously collected during the game id manual scan:
```
python3 gog_products_scan -e
```

You're now good to go! 

All 3 database tables should be populated with data. This is esentially a snapshot of all the developer/publisher names, game ids and associated installer/file entries reported by the GOG APIs.

## How do I handle update scans?

Assuming you've follwed the steps described above, you are not ready to do delta runs and detect any changes to game ids and associated installer/file entries.

All you need to do is run the corresponding batch file, depending on your OS:
```
gog_updates.bat - for Windows
./gog_updates.sh - for Linux
```

**Note:** On Windows, make sure you have added the path to the python executable to your PATH system variable before running the batch file (otherwise the OS will not know where to pick up the python executable from).

Wait for the script to finish collecting data.

You can now run the provided SQL queries to get the updates, using the provided sql script (*sql\gog_updates.sql*). Any SQLite client can be used to this purpose. I personally recommend getting **DB Browser for SQLite**: https://sqlitebrowser.org/.
