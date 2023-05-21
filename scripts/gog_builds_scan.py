#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 3.73
@date: 20/05/2023

Warning: Built for use with python 3.6+
'''

import json
import multiprocessing
import queue
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
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
#uncomment for debugging purposes only
#import traceback

##conf file block
conf_file_path = os.path.join('..', 'conf', 'gog_builds_scan.conf')

##logging configuration block
log_file_path = os.path.join('..', 'logs', 'gog_builds_scan.log')
logger_file_handler = RotatingFileHandler(log_file_path, maxBytes=25165824, backupCount=1, encoding='utf-8')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler.setFormatter(logging.Formatter(logger_format))
#logging level for other modules
logging.basicConfig(format=logger_format, level=logging.ERROR) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
#logging level defaults to INFO, but can be later modified through config file values
logger.setLevel(logging.INFO)
logger.addHandler(logger_file_handler)

##db configuration block
db_file_path = os.path.join('..', 'output_db', 'gog_gles.db')

##CONSTANTS
INSERT_BUILD_QUERY = 'INSERT INTO gog_builds VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

UPDATE_BUILD_QUERY = ('UPDATE gog_builds SET gb_int_updated = ?, '
                      'gb_int_json_payload = ?, '
                      'gb_int_json_diff = ?, '
                      'gb_total_count = ?, '
                      'gb_count = ?, '
                      'gb_main_version_names = ?, '
                      'gb_branch_version_names = ?, '
                      'gb_has_private_branches = ? '
                      'WHERE gb_int_id = ? AND gb_int_os = ?')

INSERT_INSTALLERS_DELTA_QUERY = 'INSERT INTO gog_installers_delta VALUES (?,?,?,?,?,?,?,?,?,?,?)'

UPDATE_INSTALLERS_DELTA_QUERY = ('UPDATE gog_installers_delta SET gid_int_updated = ?, ' 
                                 'gid_int_latest_galaxy_build = ?, '
                                 'gid_int_latest_installer_version = ? '
                                 'WHERE gid_int_id = ? AND gid_int_os = ? AND gid_int_fixed IS NULL')

OPTIMIZE_QUERY = 'PRAGMA optimize'

#value separator for multi-valued fields
MVF_VALUE_SEPARATOR = '; '
#supported build OSes, with valid API endpoints
SUPPORTED_OSES = ('windows', 'osx')
#number of seconds a process will wait to get/put in a queue
QUEUE_WAIT_TIMEOUT = 10 #seconds
#allow a process to fully load before starting the next process
#(helps preserve process start order)
PROCESS_START_WAIT_INTERVAL = 0.1 #seconds
HTTP_OK = 200

def sigterm_handler(signum, frame):
    #exceptions may happen here as well due to logger syncronization mayhem on shutdown
    try:
        logger.debug('Stopping scan due to SIGTERM...')
    except:
        pass
    
    raise SystemExit(0)

def sigint_handler(signum, frame):
    #exceptions may happen here as well due to logger syncronization mayhem on shutdown
    try:
        logger.debug('Stopping scan due to SIGINT...')
    except:
        pass
    
    raise SystemExit(0)
    
def gog_builds_query(process_tag, product_id, os_value, scan_mode, 
                     db_lock, session, db_connection):
    
    builds_url = f'https://content-system.gog.com/products/{product_id}/os/{os_value}/builds?generation=2'
    
    try:
        response = session.get(builds_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'{process_tag}BQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == HTTP_OK:
            try:
                json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
                
                total_count = json_parsed['total_count']
                logger.debug(f'{process_tag}BQ >>> Total count: {total_count}.')
            except:
                logger.warning(f'{process_tag}BQ >>> Unable to retrieve total_count for {product_id}, {os_value}.')
                raise Exception()
            
            if total_count > 0:
                logger.debug(f'{process_tag}BQ >>> Found builds for id {product_id}, {os_value}...')
                
                db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_builds WHERE gb_int_id = ? AND gb_int_os = ?', 
                                                  (product_id, os_value))
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
                                                               None, product_id, product_name, os_value, 
                                                               total_count, count, main_version_names, 
                                                               branch_version_names, has_private_branches))
                        db_connection.commit()
                    logger.info(f'{process_tag}BQ +++ Added a new DB entry for {product_id}: {product_name}, {os_value}.')
                
                elif entry_count == 1:
                    #do not update existing entries in a full or products scan since update/delta scans will take care of that
                    if scan_mode == 'full' or scan_mode == 'products':
                        logger.info(f'{process_tag}BQ >>> Found an existing db entry with id {product_id}, {os_value}. Skipping.')
                    #manual scans will be treated as update scans
                    else:
                        db_cursor.execute('SELECT gb_int_removed, gb_int_json_payload, gb_int_title FROM gog_builds '
                                          'WHERE gb_int_id = ? AND gb_int_os = ?', (product_id, os_value))
                        existing_removed, existing_json_formatted, existing_product_name = db_cursor.fetchone()
                        
                        if existing_removed is not None:
                            logger.debug(f'{process_tag}BQ >>> Found a previously removed entry for {product_id}, {os_value}. Clearing removed status...')
                            with db_lock:
                                db_cursor.execute('UPDATE gog_builds SET gb_int_removed = NULL WHERE gb_int_id = ? AND gb_int_os = ?', 
                                                  (product_id, os_value))
                                db_connection.commit()
                            logger.info(f'{process_tag}BQ *** Cleared removed status for {product_id}, {os_value}: {product_name}')
                        
                        if product_name is not None and existing_product_name != product_name:
                            logger.info(f'{process_tag}BQ >>> Found a valid (or new) product name: {product_name}. Updating...')
                            with db_lock:
                                db_cursor.execute('UPDATE gog_builds SET gb_int_title = ? WHERE gb_int_id = ? AND gb_int_os = ?', 
                                               (product_name, product_id, os_value))
                                db_connection.commit()
                            logger.info(f'{process_tag}BQ ~~~ Successfully updated product name for DB entry with id {product_id}, {os_value}.')
                        
                        if existing_json_formatted != json_formatted:
                            logger.debug(f'{process_tag}BQ >>> Existing entry for {product_id}, {os_value} is outdated. Updating...')
                            
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
                                                                       has_private_branches, product_id, os_value))
                                db_connection.commit()
                            logger.info(f'{process_tag}BQ ~~~ Updated the DB entry for {product_id}: {product_name}, {os_value}.')
            
            elif scan_mode == 'update' and total_count == 0:
                
                db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_builds WHERE gb_int_id = ? AND gb_int_os = ?', 
                                                  (product_id, os_value))
                entry_count = db_cursor.fetchone()[0]
                
                if entry_count == 1:
                    #check to see the existing value for gb_int_removed
                    db_cursor = db_connection.execute('SELECT gb_int_removed, gb_int_title FROM gog_builds WHERE gb_int_id = ? AND gb_int_os = ?', 
                                                      (product_id, os_value))
                    existing_delisted, product_name = db_cursor.fetchone()
                    
                    #only alter the entry if not already marked as removed
                    if existing_delisted is None:
                        logger.debug(f'{process_tag}BQ >>> All builds for {product_id}, {os_value} have been removed...')
                        with db_lock:
                            #also reset/clear all other attributes (and diff field) in order to reflect the removal;
                            #previous values will still be stored as part of the attached json payload
                            db_cursor.execute('UPDATE gog_builds SET gb_int_removed = ?, gb_int_json_diff = NULL, gb_total_count = 0, gb_count = 0, '
                                              'gb_main_version_names = NULL, gb_branch_version_names = NULL, gb_has_private_branches = 0 '
                                              'WHERE gb_int_id = ? AND gb_int_os = ?', (datetime.now(), product_id, os_value))
                            db_connection.commit()
                        logger.warning(f'{process_tag}BQ --- Marked the builds for {product_id}, {os_value}: {product_name} as removed.')
                    else:
                        logger.debug(f'{process_tag}BQ >>> Builds for {product_id}, {os_value} are already marked as removed.')
        
        else:
            logger.warning(f'{process_tag}BQ >>> HTTP error code {response.status_code} received for {product_id}, {os_value}.')
            raise Exception()
        
        return True
    
    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'{process_tag}BQ >>> Connection SSL error encountered for {product_id}, {os_value}.')
        return False
    
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'{process_tag}BQ >>> Connection error encountered for {product_id}, {os_value}.')
        return False
    
    except:
        logger.debug(f'{process_tag}BQ >>> Builds query has failed for {product_id}, {os_value}.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return False

def worker_process(process_tag, scan_mode, id_queue, db_lock, config_lock, 
                   fail_event, terminate_event):
    #catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    #catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)
    
    #only scan the first supported OS ('windows') and let update scans detect additional OS builds
    FULL_SCAN_OS_VALUE = SUPPORTED_OSES[0];
    
    processConfigParser = ConfigParser()
    
    with requests.Session() as processSession, sqlite3.connect(db_file_path) as process_db_connection:
        logger.info(f'{process_tag}>>> Starting worker process...')
        
        try:
            while not terminate_event.is_set():
                product_id = id_queue.get(True, QUEUE_WAIT_TIMEOUT)
                
                retry_counter = 0
                retries_complete = False
                
                while not retries_complete and not terminate_event.is_set():
                    if retry_counter > 0:
                        logger.debug(f'{process_tag}>>> Retry count: {retry_counter}.')
                        #main iteration incremental sleep
                        sleep((retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL)
                    
                    retries_complete = gog_builds_query(process_tag, product_id, FULL_SCAN_OS_VALUE, scan_mode, 
                                                        db_lock, processSession, process_db_connection)
                        
                    if retries_complete:
                        if retry_counter > 0:
                            logger.info(f'{process_tag}>>> Succesfully retried for {product_id}, {FULL_SCAN_OS_VALUE}.')
                    else:
                        retry_counter += 1
                        #terminate the scan if the RETRY_COUNT limit is exceeded
                        if retry_counter > RETRY_COUNT:
                            logger.critical(f'{process_tag}>>> Request most likely blocked/invalidated by GOG. Terminating process.')
                            fail_event.set()
                            terminate_event.set()
                
                if product_id % ID_SAVE_INTERVAL == 0 and not terminate_event.is_set():
                    with config_lock:
                        processConfigParser.read(conf_file_path)
                        processConfigParser['FULL_SCAN']['start_id'] = str(product_id)
                        
                        with open(conf_file_path, 'w') as file:
                            processConfigParser.write(file)
                    
                    logger.info(f'{process_tag}>>> Processed up to id: {product_id}...')
        
        #the main process has stopped populating the queue if this exception is raised
        except queue.Empty:
            logger.debug(f'{process_tag}>>> Timed out while waiting for queue.')
        
        except SystemExit:
            pass
        
        logger.info(f'{process_tag}>>> Stopping worker process...')
        
        logger.debug(f'{process_tag}>>> Running PRAGMA optimize...')
        with db_lock:
            process_db_connection.execute(OPTIMIZE_QUERY)

if __name__ == "__main__":
    #catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    #catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)
    
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
    
    configParser = ConfigParser()
    
    try:
        configParser.read(conf_file_path)
        
        #parsing generic parameters
        general_section = configParser['GENERAL']
        LOGGING_LEVEL = general_section.get('logging_level').upper()
        
        #DEBUG, INFO, WARNING, ERROR, CRITICAL
        #remains set to INFO if none of the other valid log levels are specified
        if LOGGING_LEVEL == 'INFO':
            pass
        elif LOGGING_LEVEL == 'DEBUG':
            logger.setLevel(logging.DEBUG)
        elif LOGGING_LEVEL == 'WARNING':
            logger.setLevel(logging.WARNING)
        elif LOGGING_LEVEL == 'ERROR':
            logger.setLevel(logging.ERROR)
        elif LOGGING_LEVEL == 'CRITICAL':
            logger.setLevel(logging.CRITICAL)
        
        scan_mode = general_section.get('scan_mode')
        CONF_BACKUP = general_section.get('conf_backup')
        DB_BACKUP = general_section.get('db_backup')
        HTTP_TIMEOUT = general_section.getint('http_timeout')
        RETRY_COUNT = general_section.getint('retry_count')
        RETRY_SLEEP_INTERVAL = general_section.getint('retry_sleep_interval')
        RETRY_AMPLIFICATION_FACTOR = general_section.getint('retry_amplification_factor')
    except:
        logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
        raise SystemExit(1)
    
    logger.info('*** Running BUILDS scan script ***')
    
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
    if CONF_BACKUP == 'true' or CONF_BACKUP == scan_mode:
        if os.path.exists(conf_file_path):
            #create a backup of the existing conf file - mostly for debugging/recovery
            copy2(conf_file_path, conf_file_path + '.bak')
            logger.info('Successfully created conf file backup.')
        else:
            logger.critical('Could find specified conf file!')
            raise SystemExit(2)
    
    #boolean 'true' or scan_mode specific activation
    if DB_BACKUP == 'true' or DB_BACKUP == scan_mode:
        if os.path.exists(db_file_path):
            #create a backup of the existing db - mostly for debugging/recovery
            copy2(db_file_path, db_file_path + '.bak')
            logger.info('Successfully created DB backup.')
        else:
            #subprocess.run(['python', 'gog_create_db.py'])
            logger.critical('Could find specified DB file!')
            raise SystemExit(3)
    
    ##inter-process resources locks
    db_lock = multiprocessing.Lock()
    config_lock = multiprocessing.Lock()
    ##shared process events
    terminate_event = multiprocessing.Event()
    terminate_event.clear()
    fail_event = multiprocessing.Event()
    fail_event.clear()
    
    if scan_mode == 'full':
        logger.info('--- Running in FULL scan mode ---')
        
        full_scan_section = configParser['FULL_SCAN']
        ID_SAVE_INTERVAL = full_scan_section.getint('id_save_interval')
        #number of active connection processes
        CONNECTION_PROCESSES = full_scan_section.getint('connection_processes')
        #STOP_ID = 2147483647, in order to scan the full range,
        #stopping at the upper limit of a 32 bit signed integer type
        STOP_ID = full_scan_section.getint('stop_id')
        #product_id will restart from scan_id
        product_id = full_scan_section.getint('start_id')
        #reduce starting point by a save interval to account for any process overlap
        if product_id > ID_SAVE_INTERVAL: product_id -= ID_SAVE_INTERVAL
        
        logger.info(f'Restarting scan from id: {product_id}.')
        
        stop_id_reached = False
        id_queue = multiprocessing.Queue(CONNECTION_PROCESSES * 2)
        process_list = []
        
        try:
            for process_no in range(CONNECTION_PROCESSES):
                #apply spacing to single digit process_no for nicer logging in case of 10+ processes
                PROCESS_LOGGING_FILLER = '0' if CONNECTION_PROCESSES > 9 and process_no < 9 else ''
                process_tag_nice = ''.join(('P#', PROCESS_LOGGING_FILLER, str(process_no + 1), ' '))
                
                process = multiprocessing.Process(target=worker_process, 
                                                 args=(process_tag_nice, scan_mode, id_queue, db_lock, config_lock, 
                                                       fail_event, terminate_event), 
                                                 daemon=True)
                process.start()
                process_list.append(process)
                sleep(PROCESS_START_WAIT_INTERVAL)
            
            while not stop_id_reached and not terminate_event.is_set():
                try:
                    id_queue.put(product_id, True, QUEUE_WAIT_TIMEOUT)
                    
                    logger.debug(f'Processing the following product_id: {product_id}.')
                    product_id += 1
                    
                    if product_id > STOP_ID:
                        logger.info(f'Stop id of {STOP_ID} reached. Halting processing...')
                        stop_id_reached = True
                
                except queue.Full:
                    logger.debug('Timed out on queue insert.')
        
        except SystemExit:
            try:
                terminate_event.set()
                logger.info('Stopping full scan...')
            except:
                terminate_event.set()
        
        finally:
            logger.info('Waiting for the worker processes to complete...')
            
            for process in process_list:
                process.join()
            
            logger.info('The worker processes have been stopped.')
    
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
            with requests.Session() as session, sqlite3.connect(db_file_path) as db_connection:
                #select all existing ids from the gog_builds table
                db_cursor = db_connection.execute('SELECT DISTINCT gb_int_id FROM gog_builds WHERE gb_int_removed IS NULL AND '
                                                  'gb_int_id > ? ORDER BY 1', (last_id, ))
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all applicable product ids from the DB...')
                
                #used to track the number of processed ids
                last_id_counter = 0
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    
                    for os_value in SUPPORTED_OSES:
                        retries_complete = False
                        retry_counter = 0
                        
                        while not retries_complete and not terminate_event.is_set():
                            if retry_counter > 0:
                                sleep_interval = (retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL
                                logger.info(f'Sleeping for {sleep_interval} seconds due to throttling...')
                                sleep(sleep_interval)
                            
                            retries_complete = gog_builds_query('', current_product_id, os_value, scan_mode, 
                                                                db_lock, session, db_connection)
                            
                            if retries_complete:
                                if retry_counter > 0:
                                    logger.info(f'Succesfully retried for {current_product_id}, {os_value}.')
                                
                                last_id_counter += 1
                            
                            else:
                                retry_counter += 1
                                #terminate the scan if the RETRY_COUNT limit is exceeded
                                if retry_counter > RETRY_COUNT:
                                    logger.critical('Retry count exceeded, terminating scan!')
                                    fail_event.set()
                                    terminate_event.set()
                    
                    if last_id_counter % ID_SAVE_FREQUENCY == 0 and not not terminate_event.is_set():
                        configParser.read(conf_file_path)
                        configParser['UPDATE_SCAN']['last_id'] = str(current_product_id)
                        
                        with open(conf_file_path, 'w') as file:
                            configParser.write(file)
                        
                        logger.info(f'Saved scan up to last_id of {current_product_id}.')
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping update scan...')
    
    elif scan_mode == 'products':
        logger.info('--- Running in PRODUCTS scan mode ---')
        
        #blank the filter for a really thorough scan, although it usually doesn't make sense (but not always,
        #since some 'pack' and 'dlc' entries do have builds linked to them... hopefully just GOGBears tripping)
        #GAME_TYPE_FILTER = ''
        #filtering by game_type will drastically reduce the number of scanned ids
        GAME_TYPE_FILTER = ' AND gp_game_type = \'game\''
        
        try:
            with requests.Session() as session, sqlite3.connect(db_file_path) as db_connection:
                #select all existing ids from the gog_products table which are not already present in the 
                #gog_builds table and atempt to scan them from matching builds API entries
                db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id NOT IN '
                                                  f'(SELECT DISTINCT gb_int_id FROM gog_builds ORDER BY 1)'
                                                  f'{GAME_TYPE_FILTER} ORDER BY 1')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all applicable product ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    
                    for os_value in SUPPORTED_OSES:
                        retries_complete = False
                        retry_counter = 0
                        
                        while not retries_complete and not terminate_event.is_set():
                            if retry_counter > 0:
                                sleep_interval = (retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL
                                logger.info(f'Sleeping for {sleep_interval} seconds due to throttling...')
                                sleep(sleep_interval)
                            
                            retries_complete = gog_builds_query('', current_product_id, os_value, scan_mode, 
                                                                db_lock, session, db_connection)
                            
                            if retries_complete:
                                if retry_counter > 0:
                                    logger.info(f'Succesfully retried for {current_product_id}, {os_value}.')
                            else:
                                retry_counter += 1
                                #terminate the scan if the RETRY_COUNT limit is exceeded
                                if retry_counter > RETRY_COUNT:
                                    logger.critical('Retry count exceeded, terminating scan!')
                                    fail_event.set()
                                    terminate_event.set()
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping products scan...')
    
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
            with requests.Session() as session, sqlite3.connect(db_file_path) as db_connection:
                for product_id in id_list:
                    logger.info(f'Running scan for id {product_id}...')

                    for os_value in SUPPORTED_OSES:
                        retries_complete = False
                        retry_counter = 0
                    
                        while not retries_complete and not terminate_event.is_set():
                            if retry_counter > 0:
                                logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                                sleep(RETRY_SLEEP_INTERVAL)
                                logger.warning(f'Reprocessing id {product_id}...')
                            
                            retries_complete = gog_builds_query('', product_id, os_value, scan_mode, 
                                                                db_lock, session, db_connection)
                            
                            if retries_complete:
                                if retry_counter > 0:
                                    logger.info(f'Succesfully retried for {product_id}, {os_value}.')
                            else:
                                retry_counter += 1
                                #terminate the scan if the RETRY_COUNT limit is exceeded
                                if retry_counter > RETRY_COUNT:
                                    logger.critical('Retry count exceeded, terminating scan!')
                                    fail_event.set()
                                    terminate_event.set()
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping manual scan...')
    
    elif scan_mode == 'delta':
        logger.info('--- Running in DELTA scan mode ---')
        
        #strip any punctuation or other grouping characters from builds/versions
        STRIP_OUT_LIST = [' ', ',', '.', '-', '_', '[', ']', '(', ')', '{', '}', '/', '\\']
        #static regex pattern for removing end-of-string RC identifier from builds/installers
        GOG_RC_REMOVAL_REGEX = re.compile('RC[0-9]{1}$')
        #static regex pattern for removing end-of-string GOG version strings from builds/installers
        GOG_VERSION_REMOVAL_REGEX = re.compile('GOG[0-9]{0,5}$')
        
        detected_discrepancies = {'windows': [], 'osx': []}
        
        try:
            with sqlite3.connect(db_file_path) as db_connection:
                #select all existing ids from the gog_builds table (with valid builds) that are also present in the gog_files table
                db_cursor = db_connection.execute('SELECT gb_int_id, gb_int_os, gb_int_title, gb_main_version_names FROM gog_builds '
                                                  'WHERE gb_main_version_names IS NOT NULL AND gb_int_id IN '
                                                  '(SELECT DISTINCT gf_int_id FROM gog_files WHERE gf_int_removed IS NULL ORDER BY 1)'
                                                  ' ORDER BY 1')
                delta_list = db_cursor.fetchall()
                logger.debug('Retrieved all applicable product ids from the DB...')
                
                for delta_entry in delta_list:
                    current_product_id = delta_entry[0]
                    current_os_value = delta_entry[1]
                    #'osx' compatible products have installer OS field values of 'mac', not 'osx'...
                    current_os_files = 'mac' if current_os_value == 'osx' else current_os_value
                    logger.debug(f'Now processing id {current_product_id}, {current_os_value}...')
                    
                    current_product_title = delta_entry[2]
                    
                    current_main_version_names = delta_entry[3].split(MVF_VALUE_SEPARATOR)
                    logger.debug(f'Current builds main version names are: {current_main_version_names}.')
                    
                    #restricing languages to 'en' only will solve a lot of version discrepancy problems, 
                    #as some installers get misversioned non-english languages added at later points in time, 
                    #however the following titles will no longer be tracked because of this 
                    #(mentioning them here for future reference):
                    #
                    #Kajko i Kokosz    1720224179    pl
                    #Wolfenstein II: The New Colossus German Edition    1285433790    de
                    #Anstoss 2 Gold Edition    1808817480    de
                    #ANSTOSS 3: Der Fußballmanager    1886141726    de
                    #
                    db_cursor = db_connection.execute('SELECT DISTINCT gf_version FROM gog_files WHERE gf_int_id = ? AND gf_int_removed IS NULL '
                                                      'AND gf_language = \'en\' AND gf_int_download_type = \'installer\' AND gf_os = ? '
                                                      'AND gf_version IS NOT NULL ORDER BY gf_int_added DESC LIMIT 1', 
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
                        
                        #remove any (A) identifiers from builds/installers
                        current_latest_build_version = current_latest_build_version.replace('(A)', '')
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
                        
                        #remove RCX strings
                        current_latest_build_version = GOG_RC_REMOVAL_REGEX.sub('', current_latest_build_version)
                        logger.debug(f'Post RCX comparison build version is: {current_latest_build_version}.')
                        current_latest_file_version = GOG_RC_REMOVAL_REGEX.sub('', current_latest_file_version)
                        logger.debug(f'Post RCX comparison file version is: {current_latest_file_version}.')
                        
                        #remove (GOG-X) strings
                        current_latest_build_version = GOG_VERSION_REMOVAL_REGEX.sub('', current_latest_build_version)
                        logger.debug(f'Post GOG-X comparison build version is: {current_latest_build_version}.')
                        current_latest_file_version = GOG_VERSION_REMOVAL_REGEX.sub('', current_latest_file_version)
                        logger.debug(f'Post GOG-X comparison file version is: {current_latest_file_version}.')
                        
                        #exclude any blank entries (blanked after previous filtering)
                        #as well as some weird corner-case matches due to GOG's versioning madness
                        if current_latest_file_version == '' or current_latest_build_version == '':
                            excluded = True
                        elif current_latest_build_version[0] == 'V' and current_latest_build_version[1:] == current_latest_file_version:
                            excluded = True
                        elif current_latest_build_version[-1] == 'A' and current_latest_build_version[:-1] == current_latest_file_version:
                            excluded = True
                        elif current_latest_build_version[-1] == 'G' and current_latest_build_version[:-1] == current_latest_file_version:
                            excluded = True
                        elif current_latest_file_version[0] == 'V' and current_latest_file_version[1:] == current_latest_build_version:
                            excluded = True
                        elif current_latest_file_version[-1] == 'A' and current_latest_file_version[:-1] == current_latest_build_version:
                            excluded = True
                        
                        if not excluded and current_latest_file_version != current_latest_build_version:
                            #add detected discrepancy to its OS list
                            detected_discrepancies[current_os_value].append(current_product_id)
                            #use MAX on gid_int_false_positive, although there should only ever be one entry
                            db_cursor.execute('SELECT COUNT(*), MAX(gid_int_false_positive) FROM gog_installers_delta WHERE gid_int_id = ? '
                                              'AND gid_int_os = ? AND gid_int_fixed IS NULL', (current_product_id, current_os_value))
                            installer_delta_entry_count, current_false_positive = db_cursor.fetchone()
                            
                            #false positive status should be set to False for new entries
                            current_false_positive = False if current_false_positive is None else current_false_positive
                            
                            if installer_delta_entry_count != 0:
                                db_cursor.execute('SELECT COUNT(*) FROM gog_installers_delta WHERE gid_int_id = ? AND gid_int_os = ? '
                                                  'AND gid_int_latest_galaxy_build = ? AND gid_int_latest_installer_version = ? AND gid_int_fixed IS NULL', 
                                                  (current_product_id, current_os_value, current_latest_build_version_orig, current_latest_file_version_orig))
                                installer_version_delta_entry_count = db_cursor.fetchone()[0]
                                
                                if installer_version_delta_entry_count != 0:
                                    logger.debug(f'Discrepancy already logged for {current_product_id}: {current_product_title}, {current_os_value}. Skipping.')
                                else:
                                    logger.debug(f'Found outdated discrepancy for {current_product_id}: {current_product_title}, {current_os_value}.')
                                    #gid_int_updated, gid_int_latest_galaxy_build, 
                                    #gid_int_latest_installer_version, gid_int_id, gid_int_os
                                    db_cursor.execute(UPDATE_INSTALLERS_DELTA_QUERY, (datetime.now(), current_latest_build_version_orig, 
                                                                                      current_latest_file_version_orig, current_product_id, current_os_value))
                                    db_connection.commit()
                                    logger.info(f'~~~ Successfully updated the entry for {current_product_id}: {current_product_title}, {current_os_value}.')
                                    
                                    if current_false_positive:
                                        #any update to a discrepancy should reset the false positive state of an entry, 
                                        #but leave the false positive reason in place for tracking purposes
                                        db_cursor.execute('UPDATE gog_installers_delta SET gid_int_false_positive = 0 '
                                                          'WHERE gid_int_id = ? AND gid_int_os = ? AND gid_int_fixed IS NULL',
                                                          (current_product_id, current_os_value))
                                        db_connection.commit()
                                        logger.warning(f'False positive status has been reset for {current_product_id}, {current_os_value}.')
                            
                            else:
                                logger.debug(f'Found new discrepancy for {current_product_id}: {current_product_title}, {current_os_value}.')
                                #gid_int_nr, gid_int_added, gid_int_fixed, gid_int_updated, gid_int_id, gid_int_title, 
                                #gid_int_os, gid_int_latest_galaxy_build, gid_int_latest_installer_version, 
                                #gid_int_false_positive, gid_int_false_positive_reason
                                db_cursor.execute(INSERT_INSTALLERS_DELTA_QUERY, (None, datetime.now(), None, None, current_product_id, current_product_title, 
                                                                                  current_os_value, current_latest_build_version_orig, current_latest_file_version_orig, 
                                                                                  current_false_positive, None))
                                db_connection.commit()
                                logger.info(f'+++ Successfully added an entry for {current_product_id}: {current_product_title}, {current_os_value}.')
                    
                    else:
                        logger.debug(f'Product with id {current_product_id} is on the exclusion list. Skipping.')
                
                #verify if previosly logged discrepancies have been fixed
                db_cursor.execute('SELECT DISTINCT gid_int_id, gid_int_title, gid_int_os FROM gog_installers_delta WHERE gid_int_fixed IS NULL ORDER BY 1')
                discrepancy_list = db_cursor.fetchall()
                
                for discrepancy in discrepancy_list:
                    current_product_id = discrepancy[0]
                    current_product_title = discrepancy[1]
                    current_os_value = discrepancy[2]
                    
                    if current_product_id not in detected_discrepancies[current_os_value]:
                        logger.debug(f'Discrepancy for {current_product_id}: {current_product_title}, {current_os_value} has been fixed.')
                        #also clear any existing manually set reason if a false positive entry is marked as resolved
                        db_cursor.execute('UPDATE gog_installers_delta SET gid_int_fixed = ?, gid_int_false_positive = 0, gid_int_false_positive_reason = NULL '
                                          'WHERE gid_int_id = ? AND gid_int_os = ? AND gid_int_fixed IS NULL', 
                                          (datetime.now(), current_product_id, current_os_value))
                        db_connection.commit()
                        logger.info(f'--- Successfully updated fixed status for {current_product_id}: {current_product_title}, {current_os_value}.')
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping delta scan...')
    
    elif scan_mode == 'removed':
        logger.info('--- Running in REMOVED scan mode ---')
        
        try:
            with requests.Session() as session, sqlite3.connect(db_file_path) as db_connection:
                #select all builds which are removed
                db_cursor = db_connection.execute('SELECT DISTINCT gb_int_id FROM gog_builds WHERE gb_int_removed IS NOT NULL ORDER BY 1')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all removed build ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.info(f'Running scan for id {current_product_id}...')

                    for os_value in SUPPORTED_OSES:
                        retries_complete = False
                        retry_counter = 0
                    
                        while not retries_complete and not terminate_event.is_set():
                            if retry_counter > 0:
                                logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                                sleep(RETRY_SLEEP_INTERVAL)
                                logger.warning(f'Reprocessing id {current_product_id}...')
                            
                            retries_complete = gog_builds_query('', current_product_id, os_value, scan_mode, 
                                                                db_lock, session, db_connection)
                            
                            if retries_complete:
                                if retry_counter > 0:
                                    logger.info(f'Succesfully retried for {current_product_id}, {os_value}.')
                            else:
                                retry_counter += 1
                                #terminate the scan if the RETRY_COUNT limit is exceeded
                                if retry_counter > RETRY_COUNT:
                                    logger.critical('Retry count exceeded, terminating scan!')
                                    fail_event.set()
                                    terminate_event.set()
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping removed scan...')
    
    if not terminate_event.is_set() and scan_mode == 'update':
        logger.info('Resetting last_id parameter...')
        configParser.read(conf_file_path)
        configParser['UPDATE_SCAN']['last_id'] = ''
        
        with open(conf_file_path, 'w') as file:
            configParser.write(file)
    
    logger.info('All done! Exiting...')
    
    #return a non-zero exit code if a scan failure was encountered
    if terminate_event.is_set() and fail_event.is_set():
        raise SystemExit(4)
