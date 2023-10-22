#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 4.00
@date: 22/10/2023

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
import os
from sys import argv
from shutil import copy2
from configparser import ConfigParser
from datetime import datetime
from time import sleep
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
# uncomment for debugging purposes only
#import traceback

# conf file block
CONF_FILE_PATH = os.path.join('..', 'conf', 'gog_releases_scan.conf')

# logging configuration block
LOG_FILE_PATH = os.path.join('..', 'logs', 'gog_releases_scan.log')
logger_file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=25165824, backupCount=1, encoding='utf-8')
LOGGER_FORMAT = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler.setFormatter(logging.Formatter(LOGGER_FORMAT))
# logging level for other modules
logging.basicConfig(format=LOGGER_FORMAT, level=logging.ERROR)
logger = logging.getLogger(__name__)
# logging level defaults to INFO, but can be later modified through config file values
logger.setLevel(logging.INFO) # DEBUG, INFO, WARNING, ERROR, CRITICAL
logger.addHandler(logger_file_handler)

# db configuration block
DB_FILE_PATH = os.path.join('..', 'output_db', 'gog_gles.db')

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

# value separator for multi-valued fields
MVF_VALUE_SEPARATOR = '; '
# number of seconds a process will wait to get/put in a queue
QUEUE_WAIT_TIMEOUT = 10 #seconds
# allow a process to fully load before starting the next process
# (helps preserve process start order for logging purposes)
PROCESS_START_WAIT_INTERVAL = 0.05 #seconds
HTTP_OK = 200

def sigterm_handler(signum, frame):
    # exceptions may happen here as well due to logger syncronization mayhem on shutdown
    try:
        logger.debug('Stopping scan due to SIGTERM...')
    except:
        pass

    raise SystemExit(0)

def sigint_handler(signum, frame):
    # exceptions may happen here as well due to logger syncronization mayhem on shutdown
    try:
        logger.debug('Stopping scan due to SIGINT...')
    except:
        pass

    raise SystemExit(0)

def gog_releases_query(process_tag, release_id, scan_mode, db_lock, session, db_connection):

    releases_url = f'https://gamesdb.gog.com/platforms/gog/external_releases/{release_id}'

    try:
        response = session.get(releases_url, timeout=HTTP_TIMEOUT)

        logger.debug(f'{process_tag}RQ >>> HTTP response code: {response.status_code}.')

        if response.status_code == HTTP_OK:
            if scan_mode == 'full':
                logger.info(f'{process_tag}RQ >>> Releases query for id {release_id} has returned a valid response...')

            db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_releases WHERE gr_external_id = ?', (release_id,))
            entry_count = db_cursor.fetchone()[0]

            if not (entry_count == 1 and scan_mode == 'full'):
                json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
                json_formatted = json.dumps(json_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)

                # process unmodified fields
                #release_id = json_parsed['external_id']
                release_title = json_parsed['title']['*'].strip()
                release_type = json_parsed['type']
                # process supported oses
                supported_oses = MVF_VALUE_SEPARATOR.join(sorted([os['slug'] for os in json_parsed['supported_operating_systems']]))
                if supported_oses == '': supported_oses = None
                # process genres
                genres = MVF_VALUE_SEPARATOR.join(sorted([genre['name']['*'] for genre in json_parsed['game']['genres']]))
                if genres == '': genres = None
                # process unmodified fields
                try:
                    series = json_parsed['game']['series']['name']
                except KeyError:
                    series = None
                first_release_date = json_parsed['game']['first_release_date']
                visible_in_library = json_parsed['game']['visible_in_library']
                aggregated_rating = json_parsed['game']['aggregated_rating']

            if entry_count == 0:
                with db_lock:
                    # gr_int_nr, gr_int_added, gr_int_delisted, gr_int_updated, gr_int_json_payload,
                    # gr_int_json_diff, gr_external_id, gr_title, gr_type,
                    # gr_supported_oses, gr_genres, gr_series, gr_first_release_date,
                    # gr_visible_in_library, gr_aggregated_rating
                    db_cursor.execute(INSERT_ID_QUERY, (None, datetime.now(), None, None, json_formatted,
                                                        None, release_id, release_title, release_type,
                                                        supported_oses, genres, series, first_release_date,
                                                        visible_in_library, aggregated_rating))
                    db_connection.commit()
                logger.info(f'{process_tag}RQ +++ Added a new DB entry for {release_id}: {release_title}.')

            elif entry_count == 1:
                # do not update existing entries in a full scan, since update/delta scans will take care of that
                if scan_mode == 'full':
                    logger.info(f'{process_tag}RQ >>> Found an existing db entry with id {release_id}. Skipping.')

                else:
                    db_cursor.execute('SELECT gr_int_delisted, gr_int_json_payload FROM gog_releases WHERE gr_external_id = ?', (release_id,))
                    existing_delisted, existing_json_formatted = db_cursor.fetchone()

                    # clear the delisted status if an id is relisted (should only happen rarely)
                    if existing_delisted is not None:
                        logger.debug(f'{process_tag}RQ >>> Found a previously delisted entry with id {release_id}. Removing delisted status...')
                        with db_lock:
                            db_cursor.execute('UPDATE gog_releases SET gr_int_delisted = NULL WHERE gr_external_id = ?', (release_id,))
                            db_connection.commit()
                        logger.info(f'{process_tag}RQ *** Removed delisted status for {release_id}: {release_title}.')

                    if existing_json_formatted != json_formatted:
                        logger.debug(f'{process_tag}RQ >>> Existing entry for {release_id} is outdated. Updating...')

                        # calculate the diff between the new json and the previous one
                        # (applying the diff on the new json will revert to the previous version)
                        if existing_json_formatted is not None:
                            diff_formatted = ''.join([line for line in difflib.unified_diff(json_formatted.splitlines(1),
                                                                                            existing_json_formatted.splitlines(1), n=0)])
                        else:
                            diff_formatted = None

                        with db_lock:
                            # gr_int_updated, gr_int_json_payload, gr_int_json_diff, gr_title,
                            # gr_type, gr_supported_oses, gr_genres, gr_series, gr_first_release_date,
                            # gr_visible_in_library, gr_aggregated_rating, gr_external_id (WHERE clause)
                            db_cursor.execute(UPDATE_ID_QUERY, (datetime.now(), json_formatted, diff_formatted, release_title,
                                                                release_type, supported_oses, genres, series, first_release_date,
                                                                visible_in_library, aggregated_rating, release_id))
                            db_connection.commit()
                        logger.info(f'{process_tag}RQ ~~~ Updated the DB entry for {release_id}: {release_title}.')

        # existing ids return a 404 HTTP error code on removal
        elif scan_mode == 'update' and response.status_code == 404:
            # check to see the existing value for gp_int_no_longer_listed
            db_cursor = db_connection.execute('SELECT gr_title, gr_int_delisted FROM gog_releases WHERE gr_external_id = ?', (release_id,))
            release_title, existing_delisted = db_cursor.fetchone()

            # only alter the entry if not already marked as no longer listed
            if existing_delisted is None:
                logger.debug(f'{process_tag}RQ >>> Release with id {release_id} has been delisted...')
                with db_lock:
                    # also clear diff field when marking a release as delisted
                    db_cursor.execute('UPDATE gog_releases SET gr_int_delisted = ?, gr_int_json_diff = NULL '
                                      'WHERE gr_external_id = ?', (datetime.now(), release_id))
                    db_connection.commit()
                logger.info(f'{process_tag}RQ --- Delisted the DB entry for: {release_id}: {release_title}.')
            else:
                logger.debug(f'{process_tag}RQ >>> Release with id {release_id} is already marked as delisted.')

        # unmapped ids will also return a 404 HTTP error code
        elif response.status_code == 404:
            logger.debug(f'{process_tag}RQ >>> Release with id {release_id} returned a HTTP 404 error code. Skipping.')

        else:
            logger.warning(f'{process_tag}RQ >>> HTTP error code {response.status_code} received for {release_id}.')
            raise Exception()

        return True

    # sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'{process_tag}RQ >>> Connection SSL error encountered for {release_id}.')
        return False

    # sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'{process_tag}RQ >>> Connection error encountered for {release_id}.')
        return False

    except:
        logger.debug(f'{process_tag}RQ >>> External releases query has failed for {release_id}.')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())

        return False

def worker_process(process_tag, scan_mode, id_queue, db_lock, config_lock,
                   fail_event, terminate_event):
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)

    processConfigParser = ConfigParser()

    with requests.Session() as processSession, sqlite3.connect(DB_FILE_PATH) as process_db_connection:
        logger.info(f'{process_tag}>>> Starting worker process...')

        try:
            while not terminate_event.is_set():
                product_id = id_queue.get(True, QUEUE_WAIT_TIMEOUT)

                retry_counter = 0
                retries_complete = False

                while not retries_complete and not terminate_event.is_set():
                    if retry_counter > 0:
                        logger.debug(f'{process_tag}>>> Retry count: {retry_counter}.')
                        # main iteration incremental sleep
                        sleep((retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL)

                    retries_complete = gog_releases_query(process_tag, product_id, scan_mode, db_lock,
                                                          processSession, process_db_connection)

                    if retries_complete:
                        if retry_counter > 0:
                            logger.info(f'{process_tag}>>> Succesfully retried for {product_id}.')
                    else:
                        retry_counter += 1
                        # terminate the scan if the RETRY_COUNT limit is exceeded
                        if retry_counter > RETRY_COUNT:
                            logger.critical(f'{process_tag}>>> Request most likely blocked/invalidated by GOG. Terminating process!')
                            fail_event.set()
                            terminate_event.set()

                if product_id % ID_SAVE_INTERVAL == 0 and not terminate_event.is_set():
                    with config_lock:
                        processConfigParser.read(CONF_FILE_PATH)
                        processConfigParser['FULL_SCAN']['start_id'] = str(product_id)

                        with open(CONF_FILE_PATH, 'w') as file:
                            processConfigParser.write(file)

                        logger.info(f'{process_tag}>>> Processed up to id: {product_id}...')

        # the main process has stopped populating the queue if this exception is raised
        except queue.Empty:
            logger.debug(f'{process_tag}>>> Timed out while waiting for queue.')

        except SystemExit:
            pass

        logger.info(f'{process_tag}>>> Stopping worker process...')

        logger.debug(f'{process_tag}>>> Running PRAGMA optimize...')
        with db_lock:
            process_db_connection.execute(OPTIMIZE_QUERY)

if __name__ == "__main__":
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)

    parser = argparse.ArgumentParser(description=('GOG releases scan (part of gog_gles) - a script to call publicly available GOG APIs '
                                                  'in order to retrieve releases information and updates.'))

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-f', '--full', help='Perform a full releases scan using the Galaxy external releases endpoint', action='store_true')
    group.add_argument('-u', '--update', help='Run an update scan for existing releases', action='store_true')
    group.add_argument('-p', '--products', help='Perform a products-based releases scan', action='store_true')
    group.add_argument('-m', '--manual', help='Perform a manual releases scan', action='store_true')
    group.add_argument('-r', '--removed', help='Perform a scan on all the removed releases', action='store_true')

    args = parser.parse_args()

    configParser = ConfigParser()

    try:
        configParser.read(CONF_FILE_PATH)

        # parsing generic parameters
        general_section = configParser['GENERAL']
        LOGGING_LEVEL = general_section.get('logging_level').upper()

        # DEBUG, INFO, WARNING, ERROR, CRITICAL
        # remains set to INFO if none of the other valid log levels are specified
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

    logger.info('*** Running RELEASES scan script ***')

    # detect any parameter overrides and set the scan_mode accordingly
    if len(argv) > 1:
        logger.info('Command-line parameter mode override detected.')

        if args.full:
            scan_mode = 'full'
        elif args.update:
            scan_mode = 'update'
        elif args.products:
            scan_mode = 'products'
        elif args.manual:
            scan_mode = 'manual'
        elif args.removed:
            scan_mode = 'removed'

    # boolean 'true' or scan_mode specific activation
    if CONF_BACKUP == 'true' or CONF_BACKUP == scan_mode:
        if os.path.exists(CONF_FILE_PATH):
            # create a backup of the existing conf file - mostly for debugging/recovery
            copy2(CONF_FILE_PATH, CONF_FILE_PATH + '.bak')
            logger.info('Successfully created conf file backup.')
        else:
            logger.critical('Could find specified conf file!')
            raise SystemExit(2)

    # boolean 'true' or scan_mode specific activation
    if DB_BACKUP == 'true' or DB_BACKUP == scan_mode:
        if os.path.exists(DB_FILE_PATH):
            # create a backup of the existing db - mostly for debugging/recovery
            copy2(DB_FILE_PATH, DB_FILE_PATH + '.bak')
            logger.info('Successfully created db backup.')
        else:
            #subprocess.run(['python', 'gog_create_db.py'])
            logger.critical('Could find specified DB file!')
            raise SystemExit(3)

    # inter-process resources locks
    db_lock = multiprocessing.Lock()
    config_lock = multiprocessing.Lock()
    # shared process events
    terminate_event = multiprocessing.Event()
    terminate_event.clear()
    fail_event = multiprocessing.Event()
    fail_event.clear()

    if scan_mode == 'full':
        logger.info('--- Running in FULL scan mode ---')

        full_scan_section = configParser['FULL_SCAN']
        ID_SAVE_INTERVAL = full_scan_section.getint('id_save_interval')
        # number of active connection processes
        CONNECTION_PROCESSES = full_scan_section.getint('connection_processes')
        # STOP_ID = 2147483647, in order to scan the full range,
        # stopping at the upper limit of a 32 bit signed integer type
        STOP_ID = full_scan_section.getint('stop_id')
        # product_id will restart from scan_id
        product_id = full_scan_section.getint('start_id')
        # reduce starting point by a batch to account for any process overlap
        if product_id > ID_SAVE_INTERVAL: product_id -= ID_SAVE_INTERVAL

        logger.info(f'Restarting scan from id: {product_id}.')

        stop_id_reached = False
        id_queue = multiprocessing.Queue(CONNECTION_PROCESSES * 2)
        process_list = []

        try:
            for process_no in range(CONNECTION_PROCESSES):
                # apply spacing to single digit process_no for nicer logging in case of 10+ processes
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
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                # skip releases which are no longer listed
                db_cursor = db_connection.execute('SELECT gr_external_id FROM gog_releases WHERE gr_external_id > ? '
                                                  'AND gr_int_delisted IS NULL ORDER BY 1', (last_id,))
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all existing product ids from the DB...')

                last_id_counter = 0

                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0

                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')

                        retries_complete = gog_releases_query('', current_product_id, scan_mode, db_lock,
                                                              session, db_connection)

                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')

                            last_id_counter += 1

                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()

                    if last_id_counter % ID_SAVE_FREQUENCY == 0 and not terminate_event.is_set():
                        configParser.read(CONF_FILE_PATH)
                        configParser['UPDATE_SCAN']['last_id'] = str(current_product_id)

                        with open(CONF_FILE_PATH, 'w') as file:
                            configParser.write(file)

                        logger.info(f'Saved scan up to last_id of {current_product_id}.')

                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)

        except SystemExit:
            terminate_event.set()
            logger.info('Stopping update scan...')

    elif scan_mode == 'products':
        logger.info('--- Running in PRODUCTS scan mode ---')

        products_scan_section = configParser['PRODUCTS_SCAN']
        incremental_mode = products_scan_section.get('incremental_mode')

        # ignore the store value of last_timestamp if not in incremental mode
        if incremental_mode:
            last_timestamp = products_scan_section.get('last_timestamp')
        else:
            last_timestamp = ''

        if last_timestamp != '':
            logger.info(f'Starting products scan from timestamp: {last_timestamp}.')

        try:
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                # select all existing ids from the gog_products table which are not already present in the
                # gog_releases table and atempt to scan them from matching releases API entries
                db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id NOT IN '
                                                  '(SELECT gr_external_id FROM gog_releases ORDER BY 1) '
                                                  'AND (gp_int_added > ? OR gp_int_updated > ?) ORDER BY 1',
                                                  (last_timestamp, last_timestamp))
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all applicable product ids from the DB...')

                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0

                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            sleep_interval = (retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL
                            logger.info(f'Sleeping for {sleep_interval} seconds due to throttling...')
                            sleep(sleep_interval)

                        retries_complete = gog_releases_query('', current_product_id, scan_mode, db_lock,
                                                              session, db_connection)

                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()

                db_cursor = db_connection.execute('SELECT MAX(MAX(gp_int_added), MAX(gp_int_updated)) FROM gog_products')
                last_timestamp = db_cursor.fetchone()[0]

                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)

        except SystemExit:
            terminate_event.set()
            logger.info('Stopping products scan...')

    elif scan_mode == 'manual':
        logger.info('--- Running in MANUAL scan mode ---')

        manual_scan_section = configParser['MANUAL_SCAN']
        id_list = manual_scan_section.get('id_list')

        if id_list == '':
            logger.warning('Nothing to scan!')
            raise SystemExit(0)

        try:
            id_list = [int(product_id.strip()) for product_id in id_list.split(',')]
        except ValueError:
            logger.critical('Could not parse id list!')
            raise SystemExit(4)

        try:
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                for product_id in id_list:
                    logger.info(f'Running scan for id {product_id}...')
                    retries_complete = False
                    retry_counter = 0

                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')

                        retries_complete = gog_releases_query('', product_id, scan_mode, db_lock,
                                                              session, db_connection)

                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {product_id}.')
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()

                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)

        except SystemExit:
            terminate_event.set()
            logger.info('Stopping manual scan...')

    elif scan_mode == 'removed':
        logger.info('--- Running in REMOVED scan mode ---')

        try:
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                # select all existing ids from the gog_products table which are not already present in the
                # gog_releases table and atempt to scan them from matching releases API entries
                db_cursor = db_connection.execute('SELECT gr_external_id FROM gog_releases WHERE gr_int_delisted IS NOT NULL ORDER BY 1')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all removed release ids from the DB...')

                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0

                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            sleep_interval = (retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL
                            logger.info(f'Sleeping for {sleep_interval} seconds due to throttling...')
                            sleep(sleep_interval)

                        retries_complete = gog_releases_query('', current_product_id, scan_mode, db_lock,
                                                              session, db_connection)

                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()

                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)

        except SystemExit:
            terminate_event.set()
            logger.info('Stopping removed scan...')

    if not terminate_event.is_set():
        if scan_mode == 'update':
            logger.info('Resetting last_id parameter...')
            configParser.read(CONF_FILE_PATH)
            configParser['UPDATE_SCAN']['last_id'] = ''

            with open(CONF_FILE_PATH, 'w') as file:
                configParser.write(file)

        elif scan_mode == 'products':
            logger.info('Setting new last_timestamp value...')
            configParser.read(CONF_FILE_PATH)
            configParser['PRODUCTS_SCAN']['last_timestamp'] = last_timestamp
            # also enable incremental mode for subsequent scans
            configParser['PRODUCTS_SCAN']['incremental_mode'] = 'true'

            with open(CONF_FILE_PATH, 'w') as file:
                configParser.write(file)

    logger.info('All done! Exiting...')

    # return a non-zero exit code if a scan failure was encountered
    if terminate_event.is_set() and fail_event.is_set():
        raise SystemExit(5)
