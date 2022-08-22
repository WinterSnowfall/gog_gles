#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 3.23
@date: 20/08/2022

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
import re
import os
from sys import argv
from shutil import copy2
from configparser import ConfigParser
from datetime import datetime
from time import sleep
from queue import Queue, Empty
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
conf_file_full_path = os.path.join('..', 'conf', 'gog_builds_scan.conf')

##logging configuration block
log_file_full_path = os.path.join('..', 'logs', 'gog_builds_scan.log')
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

##CONSTANTS
INSERT_BUILD_QUERY = 'INSERT INTO gog_builds VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

UPDATE_BUILD_QUERY = ('UPDATE gog_builds SET gb_int_updated = ?, '
                        'gb_int_json_payload = ?, '
                        'gb_int_json_diff = ?, '
                        'gb_total_count = ?, '
                        'gb_count = ?, '
                        'gb_main_version_names = ?, '
                        'gb_branch_version_names = ?, '
                        'gb_has_private_branches = ? WHERE gb_int_id = ? AND gb_int_os = ?')

INSERT_INSTALLERS_DELTA_QUERY = 'INSERT INTO gog_installers_delta VALUES (?,?,?,?,?,?,?,?,?)'

OPTIMIZE_QUERY = 'PRAGMA optimize'

#value separator for multi-valued fields
MVF_VALUE_SEPARATOR = '; '
#number of seconds a thread will wait to get an item from a queue
QUEUE_WAIT_TIMEOUT = 5
#thead sync timeout interval in seconds
THREAD_SYNC_TIMEOUT = 20.0

def sigterm_handler(signum, frame):
    logger.info('Stopping scan due to SIGTERM...')
    
    raise SystemExit(0)

def terminate_script():
    logger.critical('Forcefully stopping script!')
    
    #flush buffers
    os.sync()
    #forcefully terminate script process
    os.kill(os.getpid(), signal.SIGKILL)
    
def gog_builds_query(product_id, os, scan_mode, session, db_connection):
    
    builds_url = f'https://content-system.gog.com/products/{product_id}/os/{os}/builds?generation=2'
    
    try:
        response = session.get(builds_url, timeout=HTTP_TIMEOUT)
            
        logger.debug(f'BQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == 200:
            try:
                json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
       
                total_count = json_parsed['total_count']
                logger.debug(f'BQ >>> Total count: {total_count}.')
            except:
                logger.warning(f'BQ >>> Unable to retrieve total_count for {product_id}, {os}.')
                raise Exception()
            
            if total_count > 0:
                logger.debug(f'BQ >>> Found builds for id {product_id}, {os}...')
                
                db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_builds WHERE gb_int_id = ? AND gb_int_os = ?', (product_id, os))
                entry_count = db_cursor.fetchone()[0]
                
                #no need to do any processing if an entry is found in 'full' or 'products' scan modes,
                #since that entry will be skipped anyway
                if not (entry_count == 1 and (scan_mode == 'full' or scan_mode == 'products')):
                    
                    json_formatted = json.dumps(json_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                    
                    count = json_parsed['count']
                    
                    #main and branch version names splitting and annotation logic
                    if len(json_parsed['items']) != 0:
                        main_item_list = []
                        branch_item_list = []
                        
                        for item in json_parsed['items']:
                            if item['version_name'] != '':
                                current_branch = item['branch']
                                current_version_name = item['version_name']
                                #there are no blank string branches as of now, only null ones
                                if current_branch is not None:
                                    branch_item_list.append(f'{current_version_name} ||| {current_branch}')
                                else:
                                    main_item_list.append(current_version_name)

                        main_version_names = MVF_VALUE_SEPARATOR.join(main_item_list)
                        branch_version_names = MVF_VALUE_SEPARATOR.join(branch_item_list)                       
                        #older entries may contain only a single un-named version
                        if main_version_names == '': main_version_names = None
                        if branch_version_names == '': branch_version_names = None
                    else:
                        main_version_names = None
                        branch_version_names = None
                     
                    has_private_branches = json_parsed['has_private_branches']
                
                    db_cursor.execute('SELECT gp_title FROM gog_products WHERE gp_id = ?', (product_id, ))
                    result = db_cursor.fetchone()
                    #entries with just hidden builds will not link to any gog_product entry
                    product_name = result[0] if result is not None else None
                        
                if entry_count == 0:
                    #gb_int_nr, gb_int_added, gb_int_removed, gb_int_updated, gb_int_json_payload,
                    #gb_int_json_diff, gb_int_id, gb_int_title, gb_int_os,
                    #gb_total_count, gb_count, gb_main_version_names, 
                    #gb_branch_version_names, gb_has_private_branches
                    with db_lock:
                        db_cursor.execute(INSERT_BUILD_QUERY, (None, datetime.now(), None, None, json_formatted, 
                                                               None, product_id, product_name, os, 
                                                               total_count, count, main_version_names, 
                                                               branch_version_names, has_private_branches))
                        db_connection.commit()
                    logger.info(f'BQ +++ Added a new DB entry for {product_id}: {product_name}, {os}.')
                    
                elif entry_count == 1:
                    #do not update existing entries in a full or products scan since update/delta scans will take care of that
                    if scan_mode == 'full' or scan_mode == 'products':
                        logger.info(f'BQ >>> Found an existing db entry with id {product_id}, {os}. Skipping.')
                    #manual scans will be treated as update scans
                    else:
                        db_cursor.execute('SELECT gb_int_removed, gb_int_json_payload, gb_int_title FROM gog_builds '
                                          'WHERE gb_int_id = ? AND gb_int_os = ?', (product_id, os))
                        existing_removed, existing_json_formatted, existing_product_name = db_cursor.fetchone()
                        
                        if existing_removed is not None:
                            logger.debug(f'BQ >>> Found a previously removed entry for {product_id}, {os}. Clearing removed status...')
                            with db_lock:
                                db_cursor.execute('UPDATE gog_builds SET gb_int_removed = NULL WHERE gb_int_id = ? AND gb_int_os = ?', 
                                                  (product_id, os))
                                db_connection.commit()
                            logger.info(f'BQ *** Cleared removed status for {product_id}, {os}: {product_name}')
                        
                        if product_name is not None and existing_product_name != product_name:
                            logger.info(f'BQ >>> Found a valid (or new) product name: {product_name}. Updating...')
                            with db_lock:
                                db_cursor.execute('UPDATE gog_builds SET gb_int_title = ? WHERE gb_int_id = ? AND gb_int_os = ?', 
                                               (product_name, product_id, os))
                                db_connection.commit()
                            logger.info(f'BQ ~~~ Successfully updated product name for DB entry with id {product_id}, {os}.')
                        
                        if existing_json_formatted != json_formatted:
                            logger.debug(f'BQ >>> Existing entry for {product_id}, {os} is outdated. Updating...')
                            
                            #calculate the diff between the new json and the previous one
                            #(applying the diff on the new json will revert to the previous version)
                            diff_formatted = ''.join([line for line in difflib.unified_diff(json_formatted.splitlines(1), 
                                                                                           existing_json_formatted.splitlines(1), n=0)])
                            
                            #gb_int_updated, gb_int_json_payload, gb_int_json_diff,
                            #gb_total_count, gb_count, gb_main_version_names, gb_branch_version_names, 
                            #gb_has_private_branches, gb_id (WHERE clause), gb_os (WHERE clause)
                            with db_lock:
                                db_cursor.execute(UPDATE_BUILD_QUERY, (datetime.now(), json_formatted, diff_formatted, 
                                                                       total_count, count, main_version_names, branch_version_names, 
                                                                       has_private_branches, product_id, os))
                                db_connection.commit()
                            logger.info(f'BQ ~~~ Updated the DB entry for {product_id}: {product_name}, {os}.')
            
            elif scan_mode == 'update' and total_count == 0:
                
                db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_builds WHERE gb_int_id = ? AND gb_int_os = ?', (product_id, os))
                entry_count = db_cursor.fetchone()[0]
                
                if entry_count == 1:
                    #check to see the existing value for gb_int_removed
                    db_cursor = db_connection.execute('SELECT gb_int_removed, gb_int_title FROM gog_builds WHERE gb_int_id = ? AND gb_int_os = ?', 
                                                      (product_id, os))
                    existing_delisted, product_name = db_cursor.fetchone()
                    
                    #only alter the entry if not already marked as removed
                    if existing_delisted is None:
                        logger.debug(f'BQ >>> All builds for {product_id}, {os} have been removed...')
                        with db_lock:
                            #also reset/clear all other attributes in order to reflect the removal;
                            #previous values will still be stored as part of the attached json payload
                            db_cursor.execute('UPDATE gog_builds SET gb_int_removed = ?, gb_total_count = 0, gb_count = 0, '
                                              'gb_main_version_names = NULL, gb_branch_version_names = NULL, gb_has_private_branches = 0 '
                                              'WHERE gb_int_id = ? AND gb_int_os = ?', (datetime.now(), product_id, os))
                            db_connection.commit()
                        logger.warning(f'BQ --- Marked the builds for {product_id}, {os}: {product_name} as removed.')
                    else:
                        logger.debug(f'BQ >>> Builds for {product_id}, {os} are already marked as removed.')
        
        else:
            logger.warning(f'BQ >>> HTTP error code {response.status_code} received for {product_id}, {os}.')
            raise Exception()
        
        return True
    
    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'BQ >>> Connection SSL error encountered for {product_id}, {os}.')
        return False
    
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'BQ >>> Connection error encountered for {product_id}, {os}.')
        return False
    
    except:
        logger.debug(f'BQ >>> Builds query has failed for {product_id}, {os}.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return False
    
def worker_thread(thread_number, scan_mode):
    global terminate_signal
    
    queue_empty = False
    threadConfigParser = ConfigParser()
    
    with requests.Session() as threadSession:
        with sqlite3.connect(db_file_full_path) as thread_db_connection:
            logger.info(f'Starting thread T#{thread_number}...')
            
            while not queue_empty and not terminate_signal:
                try:
                    product_id, os = queue.get(True, QUEUE_WAIT_TIMEOUT)
                    
                    retry_counter = 0
                    retries_complete = False
                    
                    while not retries_complete and not terminate_signal:
                        if retry_counter > 0:                    
                            logger.debug(f'T#{thread_number} >>> Retry count: {retry_counter}.')
                            #main iternation incremental sleep
                            sleep((retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL)
                        
                        retries_complete = gog_builds_query(product_id, os, scan_mode, threadSession, thread_db_connection)
                            
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'T#{thread_number} >>> Succesfully retried for {product_id}, {os}.')
                        else:
                            retry_counter += 1
                            #terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical(f'T#{thread_number} >>> Request most likely blocked/invalidated by GOG. Terminating process.')    
                                terminate_signal = True
                                #forcefully terminate script
                                terminate_script()
                        
                    #only do product_id processing on 'windows' build scans
                    if os == 'windows' and product_id % ID_SAVE_INTERVAL == 0 and not terminate_signal:
                        with config_lock:
                            threadConfigParser.read(conf_file_full_path)
                            threadConfigParser['FULL_SCAN']['start_id'] = str(product_id)
                        
                            with open(conf_file_full_path, 'w') as file:
                                threadConfigParser.write(file)
                        
                        logger.info(f'T#{thread_number} >>> Processed up to id: {product_id}...')
                    
                    queue.task_done()
                    
                except Empty:
                    logger.debug(f'T#{thread_number} >>> Timed out while waiting for queue.')
                    queue_empty = True
                    
            logger.info(f'Stopping thread T#{thread_number}...')
            
            logger.debug('Running PRAGMA optimize...')
            thread_db_connection.execute(OPTIMIZE_QUERY)
      
##main thread start

logger.info('*** Running BUILDS scan script ***')

parser = argparse.ArgumentParser(description=('GOG builds scan (part of gog_gles) - a script to call publicly available GOG APIs '
                                              'in order to retrieve builds information and updates.'))

group = parser.add_mutually_exclusive_group()
group.add_argument('-u', '--update', help='Perform an update builds scan', action='store_true')
group.add_argument('-f', '--full', help='Perform a full builds scan', action='store_true')
group.add_argument('-p', '--products', help='Perform a products-based builds scan', action='store_true')
group.add_argument('-m', '--manual', help='Perform a manual builds scan', action='store_true')
group.add_argument('-d', '--delta', help='Produce a list of ids whose latest builds are exclusive to Galaxy', action='store_true')
group.add_argument('-r', '--removed', help='Perform a scan on all the removed builds', action='store_true')

args = parser.parse_args()
    
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
    elif args.delta:
        scan_mode = 'delta'
    elif args.removed:
        scan_mode = 'removed'

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
        logger.info('Successfully created DB backup.')
    else:
        #subprocess.run(['python', 'gog_create_db.py'])
        logger.critical('Could find specified DB file!')
        raise SystemExit(3)

if scan_mode == 'full':
    logger.info('--- Running in FULL scan mode ---')
    
    #catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    
    full_scan_section = configParser['FULL_SCAN']
    ID_SAVE_INTERVAL = full_scan_section.getint('id_save_interval')
    #number of active connection threads
    CONNECTION_THREADS = full_scan_section.getint('connection_threads')
    #stop_id = 2147483647, in order to scan the full range,
    #stopping at the upper limit of a 32 bit signed integer type
    stop_id = full_scan_section.getint('stop_id')
    #product_id will restart from scan_id
    product_id = full_scan_section.getint('start_id')
    #reduce starting point by a save interval to account for any thread overlap
    if product_id > ID_SAVE_INTERVAL: product_id -= ID_SAVE_INTERVAL
    
    logger.info(f'Restarting scan from id: {product_id}.')
    
    stop_id_reached = False
    queue = Queue(CONNECTION_THREADS * 2)
    thread_list = []
    
    try:
        for thread_no in range(CONNECTION_THREADS):
            #apply spacing to single digit thread_no for nicer logging in case of 10+ threads
            THREAD_LOGGING_FILLER = '0' if CONNECTION_THREADS > 9 and thread_no < 9 else ''
            thread_no_nice = THREAD_LOGGING_FILLER + str(thread_no + 1)
            #setting daemon threads and a max limit to the thread sync on exit interval will prevent lockups
            thread = threading.Thread(target=worker_thread, args=(thread_no_nice, scan_mode), daemon=True)
            thread.start()
            thread_list.append(thread)
    
        while not stop_id_reached and not terminate_signal:
            logger.debug(f'Processing the following product_id: {product_id}.')
            #will block by default if the queue is full
            queue.put((product_id, 'windows'))
            #will block by default if the queue is full
            queue.put((product_id, 'osx'))
            product_id += 1
        
            if product_id > stop_id:
                logger.info(f'Stop id of {stop_id} reached. Halting processing...')
                stop_id_reached = True
                
    except KeyboardInterrupt:
        terminate_signal = True
        
    finally:
        logger.info('Waiting for the worker threads to complete...')
        
        for thread in thread_list:
            thread.join(THREAD_SYNC_TIMEOUT)
            
        logger.info('The worker threads have been stopped.')
    
elif scan_mode == 'update':
    logger.info('--- Running in UPDATE scan mode ---')
    
    update_scan_section = configParser['UPDATE_SCAN']
    
    try:
        last_id = update_scan_section.getint('last_id')
    except ValueError:
        last_id = 0
    
    ID_SAVE_FREQUENCY = update_scan_section.getint('id_save_frequency')
    
    if last_id > 0:
        logger.info(f'Restarting update scan from id: {last_id}.')
        
    try:
        logger.info('Starting builds update scan on all applicable DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            #select all existing ids from the gog_builds table
            db_cursor = db_connection.execute('SELECT DISTINCT gb_int_id FROM gog_builds WHERE gb_int_removed IS NULL AND '
                                              'gb_int_id > ? ORDER BY 1', (last_id, ))
            id_list = db_cursor.fetchall()
            logger.debug('Retrieved all applicable product ids from the DB...')
            
            #used to track the number of processed ids
            last_id_counter = 0
                
            with requests.Session() as session:
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    complete_windows = False
                    complete_osx = False
                    retry_counter = 0
                    
                    while not (complete_windows and complete_osx) and not terminate_signal:
                        if retry_counter > 0:
                            sleep_interval = (retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL
                            logger.info(f'Sleeping for {sleep_interval} seconds due to throttling...')
                            sleep(sleep_interval)
                                
                        complete_windows = gog_builds_query(current_product_id, 'windows', scan_mode, session, db_connection)
                        #try other oses as well, if the 'windows' scan goes well
                        if complete_windows:
                            complete_osx = gog_builds_query(current_product_id, 'osx', scan_mode, session, db_connection)
                        
                        if complete_windows and complete_osx:
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
                            
                    if last_id_counter % ID_SAVE_FREQUENCY == 0 and not not terminate_signal:
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
    
    #blank the filter for a really thorough scan, although it usually doesn't make sense (but not always,
    #since some "pack" and "dlc" entries do have builds linked to them... hopefully just GOGBears tripping)
    #GAME_TYPE_FILTER = ''
    #filtering by game_type will drastically reduce the number of scanned ids
    GAME_TYPE_FILTER = ' AND gp_game_type = "game"'
    
    try:
        logger.info('Starting builds scan (based on products) on all applicable DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            #select all existing ids from the gog_products table which are not already present in the 
            #gog_builds table and atempt to scan them from matching builds API entries
            db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id NOT IN '
                                              f'(SELECT DISTINCT gb_int_id FROM gog_builds ORDER BY 1)'
                                              f'{GAME_TYPE_FILTER} ORDER BY 1')
            id_list = db_cursor.fetchall()
            logger.debug('Retrieved all applicable product ids from the DB...')
            
            with requests.Session() as session:
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    complete_windows = False
                    complete_osx = False
                    retry_counter = 0
                    
                    while not (complete_windows and complete_osx) and not terminate_signal:
                        if retry_counter > 0:
                            sleep_interval = (retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL
                            logger.info(f'Sleeping for {sleep_interval} seconds due to throttling...')
                            sleep(sleep_interval)
                            
                        complete_windows = gog_builds_query(current_product_id, 'windows', scan_mode, session, db_connection)
                        #try other oses as well, if the 'windows' scan goes well
                        if complete_windows:
                            complete_osx = gog_builds_query(current_product_id, 'osx', scan_mode, session, db_connection)
                        
                        if complete_windows and complete_osx:
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
    try:
        id_list = [int(product_id.strip()) for product_id in id_list.split(',')]
    except ValueError:
        logger.critical('Could not parse id list!')
        raise SystemExit(4)
    
    try:
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                for product_id in id_list:
                    logger.info(f'Running scan for id {product_id}...')
                    complete_windows = False
                    complete_osx = False
                    retry_counter = 0
                    
                    while not (complete_windows and complete_osx) and not terminate_signal:
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {product_id}...')
                            
                        complete_windows = gog_builds_query(product_id, 'windows', scan_mode, session, db_connection)
                        #try other oses as well, if the 'windows' scan goes well
                        if complete_windows:
                            complete_osx = gog_builds_query(product_id, 'osx', scan_mode, session, db_connection)
                        
                        if complete_windows and complete_osx:
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
    
elif scan_mode == 'delta':
    logger.info('--- Running in DELTA scan mode ---')
    
    #strip any punctuation or other grouping characters from builds/versions
    STRIP_OUT_LIST = [' ', ',', '.', '-', '_', '[', ']', '(', ')', '{', '}', '/', '\\']
    #static regex pattern for removing GOG version strings from builds/installers
    GOG_VERSION_REMOVAL_REGEX = re.compile('GOG[0-9]{0,5}')
    
    detected_discrepancies = {'windows': [], 'osx': []}
    
    try:
        with sqlite3.connect(db_file_full_path) as db_connection:
            #select all existing ids from the gog_builds table (with valid builds) that are also present in the gog_products table
            db_cursor = db_connection.execute('SELECT gb_int_id, gb_int_os, gb_int_title, gb_main_version_names FROM gog_builds '
                                              'WHERE gb_main_version_names IS NOT NULL AND '
                                              'gb_int_id IN (SELECT gp_id FROM gog_products ORDER BY 1) ORDER BY 1')
            delta_list = db_cursor.fetchall()
            logger.debug('Retrieved all applicable product ids from the DB...')
                 
            for delta_entry in delta_list:
                current_product_id = delta_entry[0]
                current_os = delta_entry[1]
                #'osx' compatible products have installer os field listings of 'mac', not 'osx'
                current_os_files = 'mac' if current_os == 'osx' else current_os
                logger.debug(f'Now processing id {current_product_id}, {current_os}...')
                
                current_product_title = delta_entry[2]
                
                current_main_version_names = delta_entry[3].split(MVF_VALUE_SEPARATOR)
                logger.debug(f'Current builds main version names are: {current_main_version_names}.')

                #restricing languages to "en" only will solve a lot of version discrepancy problems, 
                #as some installers get misversioned non-english languages added at later points in time, 
                #however the following titles will no longer be tracked because of this 
                #(mentioning them here for future reference):
                #
                #Kajko i Kokosz    1720224179    pl
                #Wolfenstein II: The New Colossus German Edition    1285433790    de
                #Anstoss 2 Gold Edition    1808817480    de
                #ANSTOSS 3: Der Fußballmanager    1886141726    de
                #
                db_cursor = db_connection.execute('SELECT DISTINCT gf_version FROM gog_files WHERE gf_int_id = ? AND '
                                                  'gf_int_removed IS NULL AND gf_language = "en" AND gf_int_download_type = "installer" AND '
                                                  'gf_os = ? AND gf_version IS NOT NULL ORDER BY gf_int_added DESC LIMIT 1', 
                                                  (current_product_id, current_os_files))
                latest_version = db_cursor.fetchone()
                
                if latest_version is not None:
                    current_latest_build_version_orig = current_main_version_names[0].strip()
                    logger.debug(f'Current latest main build version is: {current_latest_build_version_orig}.')
                    current_latest_file_version_orig = latest_version[0].strip()
                    logger.debug(f'Current latest file version is: {current_latest_file_version_orig}.')
                    
                    excluded = False
                    
                    #convert to uppercase for comparisons
                    current_latest_build_version = current_latest_build_version_orig.upper()
                    current_latest_file_version = current_latest_file_version_orig.upper()
                                        
                    #remove any (A) identifier from build versions
                    current_latest_build_version = current_latest_build_version.replace('(A)', '')
                    #remove any (A) identifier from file versions
                    current_latest_file_version = current_latest_file_version.replace('(A)', '')
                    
                    #remove any 'GALAXY HOTFIX' and 'GOG HOTFIX' strings from build versions
                    current_latest_build_version = current_latest_build_version.replace('GALAXY HOTFIX', '')
                    current_latest_build_version = current_latest_build_version.replace('GOG HOTFIX', '')

                    #remove punctuation/formatting/grouping characters
                    for stripped_item in STRIP_OUT_LIST:
                        current_latest_build_version = current_latest_build_version.replace(stripped_item, '')
                        current_latest_file_version = current_latest_file_version.replace(stripped_item, '')
                        
                    #strip any version/build set that starts with the letter 'V'
                    if current_latest_build_version.startswith('V') and current_latest_file_version.startswith('V'):
                        current_latest_build_version = current_latest_build_version[1:]
                        current_latest_file_version = current_latest_file_version[1:]
                        
                    #strip any version/build set that ends with the letter 'A'
                    if current_latest_build_version.endswith('A') and current_latest_file_version.endswith('A'):
                        current_latest_build_version = current_latest_build_version[:-1]
                        current_latest_file_version = current_latest_file_version[:-1]
                    
                    #remove (GOG-X) strings
                    current_latest_build_version = GOG_VERSION_REMOVAL_REGEX.sub('', current_latest_build_version)
                    logger.debug(f'Comparison build version is: {current_latest_build_version}.')
                    current_latest_file_version = GOG_VERSION_REMOVAL_REGEX.sub('', current_latest_file_version)
                    logger.debug(f'Comparison file version is: {current_latest_file_version}.')
                    
                    #exclude any blank entries (blanked after previous filtering)
                    #as well as some weird corner-case matches due to GOG's versioning madness
                    if current_latest_file_version == '' or current_latest_build_version == '':
                        excluded = True
                    elif current_latest_build_version[0] == 'V' and current_latest_build_version[1:] == current_latest_file_version:
                        excluded = True
                    elif current_latest_build_version[-1] == 'A' and current_latest_build_version[:-1] == current_latest_file_version:
                        excluded = True
                    elif current_latest_file_version[-1] == 'A' and current_latest_file_version[:-1] == current_latest_build_version:
                        excluded = True
                        
                    if not excluded and current_latest_file_version != current_latest_build_version:
                        #add detected discrepancy to its os list
                        detected_discrepancies[current_os].append(current_product_id)
                        #use MAX on gid_int_false_positive, although there should only ever be one entry
                        db_cursor.execute('SELECT COUNT(*), MAX(gid_int_false_positive) FROM gog_installers_delta WHERE gid_int_id = ? '
                                          'AND gid_int_os = ? AND gid_int_fixed IS NULL', (current_product_id, current_os))
                        installer_delta_entry_count, current_false_positive = db_cursor.fetchone()
                        
                        #false positive status should be set to False for new entries
                        current_false_positive = False if current_false_positive is None else current_false_positive
                        
                        if installer_delta_entry_count != 0:
                            db_cursor.execute('SELECT COUNT(*) FROM gog_installers_delta WHERE gid_int_id = ? AND gid_int_os = ? '
                                              'AND gid_int_latest_galaxy_build = ? AND gid_int_latest_installer_version = ? AND gid_int_fixed IS NULL', 
                                              (current_product_id, current_os, current_latest_build_version_orig, current_latest_file_version_orig))
                            installer_version_delta_entry_count = db_cursor.fetchone()[0]
                            
                            if installer_version_delta_entry_count != 0:
                                logger.debug(f'Discrepancy already logged for {current_product_id}: {current_product_title}, {current_os}. Skipping.')
                            else:
                                logger.debug(f'Found outdated discrepancy for {current_product_id}: {current_product_title}, {current_os}.')
                                
                                if current_false_positive:
                                    #any updates to a discrepancy should reset the false positive state of an entry
                                    current_false_positive = False
                                    logger.warning(f'False positive status has been reset for {current_product_id}, {current_os}.')
                                
                                db_cursor.execute('UPDATE gog_installers_delta SET gid_int_latest_galaxy_build = ?, gid_int_latest_installer_version = ?, '
                                                  'gid_int_false_positive = ? WHERE gid_int_id = ? AND gid_int_os = ? AND gid_int_fixed IS NULL', 
                                                  (current_latest_build_version_orig, current_latest_file_version_orig, 
                                                   current_false_positive, current_product_id, current_os))
                                db_connection.commit()
                                logger.info(f'~~~ Successfully updated the entry for {current_product_id}: {current_product_title}, {current_os}.')
                        else:
                            logger.debug(f'Found new discrepancy for {current_product_id}: {current_product_title}, {current_os}.')
                            #gid_int_nr, gid_int_added, gid_int_fixed, gid_int_id, gid_int_title, 
                            #gid_int_os, gid_int_latest_galaxy_build, gid_int_latest_installer_version
                            #gid_int_false_positive
                            db_cursor.execute(INSERT_INSTALLERS_DELTA_QUERY, (None, datetime.now(), None, current_product_id, current_product_title, 
                                                                              current_os, current_latest_build_version_orig, current_latest_file_version_orig, 
                                                                              current_false_positive))
                            db_connection.commit()
                            logger.info(f'+++ Successfully added an entry for {current_product_id}: {current_product_title}, {current_os}.')
                
                else:
                    logger.debug(f'Product with id {current_product_id} is on the exclusion list. Skipping.')
            
            #verify if previosly logged discrepancies have been fixed
            db_cursor.execute('SELECT DISTINCT gid_int_id, gid_int_title, gid_int_os FROM gog_installers_delta WHERE gid_int_fixed IS NULL ORDER BY 1')
            discrepancy_list = db_cursor.fetchall()
            
            for discrepancy in discrepancy_list:
                current_product_id = discrepancy[0]
                current_product_title = discrepancy[1]
                current_os = discrepancy[2]
                
                if current_product_id not in detected_discrepancies[current_os]:
                    logger.debug(f'Discrepancy for {current_product_id}: {current_product_title}, {current_os} has been fixed.')
                    db_cursor.execute('UPDATE gog_installers_delta SET gid_int_fixed = ?, gid_int_false_positive = 0 WHERE gid_int_id = ? AND gid_int_os = ? '
                                      'AND gid_int_fixed IS NULL', (datetime.now(), current_product_id, current_os))
                    db_connection.commit()
                    logger.info(f'--- Successfully updated fixed status for {current_product_id}: {current_product_title}, {current_os}.')
                
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        terminate_signal = True
        
elif scan_mode == 'removed':
    logger.info('--- Running in REMOVED scan mode ---')
    
    try:
        logger.info('Starting scan on all removed DB entries...')
        
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                #select all builds which are removed
                db_cursor = db_connection.execute('SELECT DISTINCT gb_int_id FROM gog_builds WHERE gb_int_removed IS NOT NULL ORDER BY 1')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all removed build ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.info(f'Running scan for id {current_product_id}...')
                    complete_windows = False
                    complete_osx = False
                    retry_counter = 0
                    
                    while not (complete_windows and complete_osx) and not terminate_signal:
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')
                            
                        complete_windows = gog_builds_query(current_product_id, 'windows', scan_mode, session, db_connection)
                        #try other oses as well, if the 'windows' scan goes well
                        if complete_windows:
                            complete_osx = gog_builds_query(current_product_id, 'osx', scan_mode, session, db_connection)
                        
                        if complete_windows and complete_osx:
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
    
if not terminate_signal and scan_mode == 'update':
    logger.info('Resetting last_id parameter...')
    configParser.read(conf_file_full_path)
    configParser['UPDATE_SCAN']['last_id'] = ''
                    
    with open(conf_file_full_path, 'w') as file:
        configParser.write(file)
        
logger.info('All done! Exiting...')

##main thread end
