#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 3.20
@date: 16/07/2022

Warning: Built for use with python 3.6+
'''

import json
import threading
import sqlite3
import signal
import requests
import logging
import argparse
import difflib
import os
from sys import argv
from shutil import copy2
from configparser import ConfigParser
from datetime import datetime
from time import sleep
from queue import Queue
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
#uncomment for debugging purposes only
#import traceback

##global parameters init
configParser = ConfigParser()
db_lock = threading.Lock()
config_lock = threading.Lock()
terminate_signal = False

##conf file block
conf_file_full_path = os.path.join('..', 'conf', 'gog_releases_scan.conf')

##logging configuration block
log_file_full_path = os.path.join('..', 'logs', 'gog_releases_scan.log')
logger_file_handler = RotatingFileHandler(log_file_full_path, maxBytes=8388608, backupCount=1, encoding='utf-8')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler.setFormatter(logging.Formatter(logger_format))
#logging level for other modules
logging.basicConfig(format=logger_format, level=logging.ERROR) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
#logging level for current logger
logger.setLevel(logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger.addHandler(logger_file_handler)

##db configuration block
db_file_full_path = os.path.join('..', 'output_db', 'gog_gles.db')

INSERT_ID_QUERY = 'INSERT INTO gog_releases VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

UPDATE_ID_QUERY = ('UPDATE gog_releases SET gr_int_updated = ?, '
                   'gr_int_json_payload = ?, '
                   'gr_int_json_diff = ?, '
                   'gr_title = ?, '
                   'gr_type = ?, '
                   'gr_supported_oses = ?, '
                   'gr_genres = ?, '
                   'gr_series = ?, '
                   'gr_first_release_date = ?, '
                   'gr_visible_in_library = ?, '
                   'gr_aggregated_rating = ? WHERE gr_external_id = ?')

OPTIMIZE_QUERY = 'PRAGMA optimize'

#value separator for multi-valued fields
MVF_VALUE_SEPARATOR = '; '

def sigterm_handler(signum, frame):
    logger.info('Stopping scan due to SIGTERM...')
    
    raise SystemExit(0)

def terminate_script():
    logger.critical('Forcefully stopping script!')
    
    #flush buffers
    os.sync()
    #forcefully terminate script process
    os.kill(os.getpid(), signal.SIGKILL)
    
def gog_releases_query(release_id, scan_mode, session, db_connection):
    
    releases_url = f'https://gamesdb.gog.com/platforms/gog/external_releases/{release_id}'
    
    try:
        response = session.get(releases_url, timeout=HTTP_TIMEOUT)
            
        logger.debug(f'RQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == 200:
            if scan_mode == 'full':
                logger.info(f'RQ >>> Releases query for id {release_id} has returned a valid response...')
            
            db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_releases WHERE gr_external_id = ?', (release_id, ))
            entry_count = db_cursor.fetchone()[0]
            
            if not (entry_count == 1 and scan_mode == 'full'):
                
                json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
                json_formatted = json.dumps(json_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                
                #process unmodified fields
                #release_id = json_parsed['external_id']
                release_title = json_parsed['title']['*'].strip()
                release_type = json_parsed['type']
                #process supported oses
                supported_oses = MVF_VALUE_SEPARATOR.join(sorted([os['slug'] for os in json_parsed['supported_operating_systems']]))
                if supported_oses == '': supported_oses = None
                #process genres
                genres = MVF_VALUE_SEPARATOR.join(sorted([genre['name']['*'] for genre in json_parsed['game']['genres']]))
                if genres == '': genres = None
                #process unmodified fields
                try:
                    series = json_parsed['game']['series']['name']
                except KeyError:
                    series = None
                first_release_date = json_parsed['game']['first_release_date']
                visible_in_library = json_parsed['game']['visible_in_library']
                aggregated_rating = json_parsed['game']['aggregated_rating']
                    
            if entry_count == 0:
                with db_lock:
                    #gr_int_nr, gr_int_added, gr_int_delisted, gr_int_updated, gr_int_json_payload, 
                    #gr_int_json_diff, gr_external_id, gr_title, gr_type, 
                    #gr_supported_oses, gr_genres, gr_series, gr_first_release_date, 
                    #gr_visible_in_library, gr_aggregated_rating
                    db_cursor.execute(INSERT_ID_QUERY, (None, datetime.now(), None, None, json_formatted,
                                                        None, release_id, release_title, release_type, 
                                                        supported_oses, genres, series, first_release_date, 
                                                        visible_in_library, aggregated_rating))
                    db_connection.commit()
                logger.info(f'PQ +++ Added a new DB entry for {release_id}: {release_title}.')
                
            elif entry_count == 1:
                #do not update existing entries in a full scan, since update/delta scans will take care of that
                if scan_mode == 'full':
                    logger.info(f'PQ >>> Found an existing db entry with id {release_id}. Skipping.')
        
                else:
                    db_cursor.execute('SELECT gr_int_json_payload FROM gog_releases WHERE gr_external_id = ?', (release_id, ))
                    existing_json_formatted = db_cursor.fetchone()[0]
                    
                    if existing_json_formatted != json_formatted:
                        logger.debug(f'PQ >>> Existing entry for {release_id} is outdated. Updating...')
                        
                        #calculate the diff between the new json and the previous one
                        #(applying the diff on the new json will revert to the previous version)
                        if existing_json_formatted is not None:
                            diff_formatted = ''.join([line for line in difflib.unified_diff(json_formatted.splitlines(1), 
                                                                                            existing_json_formatted.splitlines(1), n=0)])
                        else:
                            diff_formatted = None
                        
                        with db_lock:
                            #gr_int_updated, gr_int_json_payload, gr_int_json_diff, gr_title, 
                            #gr_type, gr_supported_oses, gr_genres, gr_series, gr_first_release_date, 
                            #gr_visible_in_library, gr_aggregated_rating, gr_external_id (WHERE clause)
                            db_cursor.execute(UPDATE_ID_QUERY, (datetime.now(), json_formatted, diff_formatted, release_title, 
                                                                release_type, supported_oses, genres, series, first_release_date, 
                                                                visible_in_library, aggregated_rating, release_id))
                            db_connection.commit()
                        logger.info(f'PQ ~~~ Updated the DB entry for {release_id}: {release_title}.')
        
        #existing ids return a 404 HTTP error code on removal
        elif scan_mode == 'update' and response.status_code == 404:
            #check to see the existing value for gp_int_no_longer_listed
            db_cursor = db_connection.execute('SELECT gr_title, gr_int_delisted FROM gog_releases WHERE gr_external_id = ?', (release_id, ))
            release_title, existing_delisted = db_cursor.fetchone()
            
            #only alter the entry if not already marked as no longer listed
            if existing_delisted is None:
                logger.debug(f'PQ >>> Release with id {release_id} has been delisted...')
                with db_lock:
                    db_cursor.execute('UPDATE gog_releases SET gr_int_delisted = ? WHERE gr_external_id = ?', (datetime.now(), release_id))
                    db_connection.commit()
                logger.info(f'PQ --- Delisted the DB entry for: {release_id}: {release_title}.')
            else:
                logger.debug(f'PQ >>> Release with id {release_id} is already marked as delisted.')
                        
        #unmapped ids will also return a 404 HTTP error code
        elif response.status_code == 404:
            logger.debug(f'RQ >>> Release with id {release_id} returned a HTTP 404 error code. Skipping.')
        
        else:
            logger.warning(f'RQ >>> HTTP error code {response.status_code} received for {release_id}.')
            raise Exception()
        
        return True
    
    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'RQ >>> Connection SSL error encountered for {release_id}.')
        return False
    
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'RQ >>> Connection error encountered for {release_id}.')
        return False
    
    except:
        logger.debug(f'RQ >>> External releases query has failed for {release_id}.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        
        return False
    
def worker_thread(thread_number, scan_mode):
    global terminate_signal
    
    threadConfigParser = ConfigParser()
        
    with requests.Session() as threadSession:
        with sqlite3.connect(db_file_full_path) as thread_db_connection:
            while not terminate_signal:
                product_id = queue.get()
                
                retry_counter = 0
                retries_complete = False
                
                while not retries_complete and not terminate_signal:
                    if retry_counter > 0:
                        logger.debug(f'T#{thread_number} >>> Retry count: {retry_counter}.')
                        #main iternation incremental sleep
                        sleep((retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL)
                    
                    retries_complete = gog_releases_query(product_id, scan_mode, threadSession, thread_db_connection)
                    
                    if retries_complete:
                        if retry_counter > 0:
                            logger.info(f'T#{thread_number} >>> Succesfully retried for {product_id}.')
                    else:
                        retry_counter += 1
                        #terminate the scan if the RETRY_COUNT limit is exceeded
                        if retry_counter > RETRY_COUNT:
                            logger.critical(f'T#{thread_number} >>> Request most likely blocked/invalidated by GOG. Terminating process!')    
                            terminate_signal = True
                            #forcefully terminate script
                            terminate_script()
                    
                if not terminate_signal and product_id != 0 and product_id % ID_SAVE_INTERVAL == 0:
                    with config_lock:
                        threadConfigParser.read(conf_file_full_path)
                        threadConfigParser['FULL_SCAN']['start_id'] = str(product_id)
                        
                        with open(conf_file_full_path, 'w') as file:
                            threadConfigParser.write(file)
                            
                        logger.info(f'T#{thread_number} >>> Processed up to id: {product_id}...')
                
                queue.task_done()

            logger.debug('Running PRAGMA optimize...')
            thread_db_connection.execute(OPTIMIZE_QUERY)
            
##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description=('GOG releases scan (part of gog_gles) - a script to call publicly available GOG APIs '
                                              'in order to retrieve releases information and updates.'))

group = parser.add_mutually_exclusive_group()
group.add_argument('-u', '--update', help='Run an update scan for existing releases', action='store_true')
group.add_argument('-f', '--full', help='Perform a full releases scan using the Galaxy external releases endpoint', action='store_true')
group.add_argument('-p', '--products', help='Perform a products-based releases scan', action='store_true')
group.add_argument('-m', '--manual', help='Perform a manual releases scan', action='store_true')

args = parser.parse_args()

logger.info('*** Running RELEASES scan script ***')
    
try:
    #reading from config file
    configParser.read(conf_file_full_path)
    general_section = configParser['GENERAL']
    #parsing generic parameters
    conf_backup = general_section.get('conf_backup')
    db_backup = general_section.get('db_backup')
    scan_mode = general_section.get('scan_mode')
    #parsing constants
    HTTP_TIMEOUT = general_section.getint('http_timeout')
    RETRY_COUNT = general_section.getint('retry_count')
    RETRY_SLEEP_INTERVAL = general_section.getint('retry_sleep_interval')
    RETRY_AMPLIFICATION_FACTOR = general_section.getint('retry_amplification_factor')
except:
    logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
    raise SystemExit(1)

#detect any parameter overrides and set the scan_mode accordingly
if len(argv) > 1:
    logger.info('Command-line parameter mode override detected.')
    
    if args.update:
        scan_mode = 'update'
    elif args.full:
        scan_mode = 'full'
    elif args.products:
        scan_mode = 'products'
    elif args.manual:
        scan_mode = 'manual'

#boolean 'true' or scan_mode specific activation
if conf_backup == 'true' or conf_backup == scan_mode:
    if os.path.exists(conf_file_full_path):
        #create a backup of the existing conf file - mostly for debugging/recovery
        copy2(conf_file_full_path, conf_file_full_path + '.bak')
        logger.info('Successfully created conf file backup.')
    else:
        logger.critical('Could find specified conf file!')
        raise SystemExit(2)

#boolean 'true' or scan_mode specific activation
if db_backup == 'true' or db_backup == scan_mode:
    if os.path.exists(db_file_full_path):
        #create a backup of the existing db - mostly for debugging/recovery
        copy2(db_file_full_path, db_file_full_path + '.bak')
        logger.info('Successfully created db backup.')
    else:
        #subprocess.run(['python', 'gog_create_db.py'])
        logger.critical('Could find specified DB file!')
        raise SystemExit(3)
    
if scan_mode == 'full':
    logger.info('--- Running in FULL scan mode ---')
    
    #catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    
    #theads sync (on exit) timeout interval (seconds)
    THREAD_SYNC_TIMEOUT = 30
    
    full_scan_section = configParser['FULL_SCAN']
    ID_SAVE_INTERVAL = full_scan_section.getint('id_save_interval')
    #number of active connection threads
    CONNECTION_THREADS = full_scan_section.getint('connection_threads')
    #stop_id = 2147483647, in order to scan the full range,
    #stopping at the upper limit of a 32 bit signed integer type
    stop_id = full_scan_section.getint('stop_id')
    #product_id will restart from scan_id
    product_id = full_scan_section.getint('start_id')
    #reduce starting point by a batch to account for any thread overlap
    if product_id > ID_SAVE_INTERVAL: product_id -= ID_SAVE_INTERVAL
    
    logger.info(f'Restarting scan from id: {product_id}.')
    
    queue = Queue(CONNECTION_THREADS * 2)
    
    try:
        for thread_no in range(CONNECTION_THREADS):
            #apply spacing to single digit thread_no for nicer logging in case of 10+ threads
            THREAD_LOGGING_FILLER = '0' if CONNECTION_THREADS > 9 and thread_no < 9 else ''
            thread_no_nice = THREAD_LOGGING_FILLER + str(thread_no + 1)
            
            logger.info(f'Starting thread T#{thread_no_nice}...')
            #setting daemon threads and a max limit to the thread sync on exit interval will prevent lockups
            thread = threading.Thread(target=worker_thread, args=(thread_no_nice, scan_mode), daemon=True)
            thread.start()
    
        while not terminate_signal and product_id <= stop_id:
            logger.debug(f'Processing the following product_id: {product_id}.')
            queue.put(product_id)
            product_id += 1
                
        #simulate a regular keyboard stop when stop_id is reached
        if product_id > stop_id:
            logger.info(f'Stop id of {stop_id} reached. Halting processing...')
            
            #write the stop_id as the start_id in the config file
            configParser.read(conf_file_full_path)
            configParser['RELEASES_SCAN']['start_id'] = str(product_id)
            
            with open(conf_file_full_path, 'w') as file:
                configParser.write(file)
            
            raise KeyboardInterrupt
                
    except KeyboardInterrupt:
        terminate_signal = True
        terminate_sync_counter = 0
        
        logger.info('Waiting for all threads to complete...')
        #sleep until all threads except the main thread finish processing
        while threading.activeCount() > 1 and terminate_sync_counter <= THREAD_SYNC_TIMEOUT:
            sleep(1)
            terminate_sync_counter += 1
        
        if terminate_sync_counter > THREAD_SYNC_TIMEOUT:
            logger.warning('Thread sync on exit interval exceeded! Any stuck threads will now be terminated.')
            
elif scan_mode == 'update':
    logger.info('--- Running in UPDATE scan mode ---')
    
    update_scan_section = configParser['UPDATE_SCAN']
    last_id = update_scan_section.getint('last_id')
    ID_SAVE_FREQUENCY = update_scan_section.getint('id_save_frequency')
    
    if last_id > 0:
        logger.info(f'Restarting update scan from id: {last_id}.')
    
    try:
        logger.info('Starting update scan on all existing DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            #skip releases which are no longer listed
            db_cursor = db_connection.execute('SELECT gr_external_id FROM gog_releases WHERE gr_external_id > ? '
                                              'AND gr_int_delisted IS NULL ORDER BY 1', (last_id, ))
            id_list = db_cursor.fetchall()
            logger.debug('Retrieved all existing product ids from the DB...')
            
            #track the number of processed ids
            last_id_counter = 0
                
            with requests.Session() as session:
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_signal:
                        if retry_counter > 0:
                            logger.warning(f'Reprocessing id {current_product_id}...')
                            #allow a short respite before re-processing
                            sleep(2)
                            
                        retries_complete = gog_releases_query(current_product_id, scan_mode, session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                            
                            last_id_counter += 1
                            
                        else:
                            retry_counter += 1
                            #terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                terminate_signal = True
                                #forcefully terminate script
                                terminate_script()
                            
                    if not terminate_signal and last_id_counter != 0 and last_id_counter % ID_SAVE_FREQUENCY == 0:
                        configParser.read(conf_file_full_path)
                        configParser['UPDATE_SCAN']['last_id'] = str(current_product_id)
                        
                        with open(conf_file_full_path, 'w') as file:
                            configParser.write(file)
                            
                        logger.info(f'Saved scan up to last_id of {current_product_id}.')
            
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        terminate_signal = True
            
elif scan_mode == 'products':
    logger.info('--- Running in PRODUCTS scan mode ---')
    
    try:
        logger.info('Starting releases scan (based on products) on all applicable DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            #select all existing ids from the gog_products table which are not already present in the 
            #gog_releases table and atempt to scan them from matching releases API entries
            db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id NOT IN '
                                              '(SELECT gr_external_id FROM gog_releases ORDER BY 1) ORDER BY 1')
            id_list = db_cursor.fetchall()
            logger.debug('Retrieved all applicable product ids from the DB...')
            
            with requests.Session() as session:
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_signal:
                        if retry_counter > 0:
                            sleep_interval = (retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL
                            logger.info(f'Sleeping for {sleep_interval} seconds due to throttling...')
                            sleep(sleep_interval)
                            
                        retries_complete = gog_releases_query(current_product_id, scan_mode, session, db_connection)
                            
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                        else:
                            retry_counter += 1
                            #terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                terminate_signal = True
                                #forcefully terminate script
                                terminate_script()
            
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
        
    except KeyboardInterrupt:
        terminate_signal = True
    
elif scan_mode == 'manual':
    logger.info('--- Running in MANUAL scan mode ---')
    
    manual_scan_section = configParser['MANUAL_SCAN']
    #load the product id list to process
    id_list = manual_scan_section.get('id_list')
    id_list = [int(product_id.strip()) for product_id in id_list.split(',')]
    
    try:
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                for product_id in id_list:
                    logger.info(f'Running scan for id {product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_signal:
                        if retry_counter > 0:
                            logger.warning(f'Reprocessing id {product_id}...')
                            #allow a short respite before re-processing
                            sleep(2)
                        
                        retries_complete = gog_releases_query(product_id, scan_mode, session, db_connection)
                            
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {product_id}.')
                        else:
                            retry_counter += 1
                            #terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                terminate_signal = True
                                #forcefully terminate script
                                terminate_script()
            
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        terminate_signal = True
    
if not terminate_signal and scan_mode == 'update':
    logger.info('Resetting last_id parameter...')
    configParser.read(conf_file_full_path)
    configParser['UPDATE_SCAN']['last_id'] = '0'
                    
    with open(conf_file_full_path, 'w') as file:
        configParser.write(file)

logger.info('All done! Exiting...')

##main thread end
