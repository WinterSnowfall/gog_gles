#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 3.80
@date: 12/06/2023

Warning: Built for use with python 3.6+
'''

import json
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
CONF_FILE_PATH = os.path.join('..', 'conf', 'gog_ratings_scan.conf')

# logging configuration block
LOG_FILE_PATH = os.path.join('..', 'logs', 'gog_ratings_scan.log')
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

# CONSTANTS
INSERT_RATING_QUERY = 'INSERT INTO gog_ratings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

UPDATE_RATING_QUERY = ('UPDATE gog_ratings SET grt_int_updated = ?, '
                       'grt_int_json_payload = ?, '
                       'grt_int_json_diff = ?, '
                       'grt_review_count = ?, '
                       'grt_avg_rating = ?, '
                       'grt_avg_rating_count = ?, '
                       'grt_avg_rating_verified_owner = ?, '
                       'grt_avg_rating_verified_owner_count = ?, '
                       'grt_is_reviewable = ? WHERE grt_int_id = ?')

OPTIMIZE_QUERY = 'PRAGMA optimize'

HTTP_OK = 200

def sigterm_handler(signum, frame):
    logger.debug('Stopping scan due to SIGTERM...')
    
    raise SystemExit(0)

def sigint_handler(signum, frame):
    logger.debug('Stopping scan due to SIGINT...')
    
    raise SystemExit(0)

def gog_ratings_query(product_id, is_verified, session):
    
    ratings_url = f'https://reviews.gog.com/v1/products/{product_id}/averageRating'
    
    if is_verified:
        ratings_url = ''.join((ratings_url, '?reviewer=verified_owner'))
    
    logger.debug(f'RTQ >>> Querying url: {ratings_url}.')
    
    try:
        response = session.get(ratings_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'RTQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == HTTP_OK:
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            value = json_parsed['value']
            count = json_parsed['count']
        
        return (value, count, True)
    
    # sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'RTQ >>> Connection SSL error encountered for {product_id}.')
        return (None, None, False)
    
    # sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'RTQ >>> Connection error encountered for {product_id}.')
        return (None, None, False)
    
    except:
        logger.debug(f'RTQ >>> Ratings query has failed for {product_id}.')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        
        return (None, None, False)

def gog_reviews_query(product_id, session, db_connection):
    # limit the query to only one result in the english language, 
    # which will return the most helpful review (because of desc:votes)
    reviews_url = f'https://reviews.gog.com/v1/products/{product_id}/reviews?language=in:en-US&limit=1&order=desc:votes'
    
    try:
        response = session.get(reviews_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'RVQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == HTTP_OK:
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            pages = json_parsed['pages']
            logger.debug(f'RVQ >>> Pages: {pages}.')
            
            db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_ratings WHERE grt_int_id = ?', (product_id,))
            entry_count = db_cursor.fetchone()[0]
            
            if pages > 0:
                logger.debug(f'RVQ >>> Found something for id {product_id}...')
                
                json_formatted = json.dumps(json_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                
                # process unmodified fields
                review_count = json_parsed['reviewCount']
                is_reviewable = json_parsed['isReviewable']
                # get the overall ratings
                ratings_found = False
                ratings_retries = 0
                while not ratings_found:
                    if ratings_retries > 0:
                        logger.warning(f'RVQ >>> Ratings retry number {ratings_retries}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                        sleep(RETRY_SLEEP_INTERVAL)
                    avg_rating, avg_rating_count, ratings_found = gog_ratings_query(product_id, False, session)
                    if not ratings_found:
                        ratings_retries += 1
                    elif ratings_retries > 0:
                        logger.info(f'RVQ >>> Successfully retried for {product_id}.')
                # get the overall ratings for verified owners
                ratings_found = False
                ratings_retries = 0
                while not ratings_found:
                    if ratings_retries > 0:
                        logger.warning(f'RVQ >>> Ratings (verified owner) retry number {ratings_retries}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                        sleep(RETRY_SLEEP_INTERVAL)
                    avg_rating_verified_owner, avg_rating_verified_owner_count, ratings_found = gog_ratings_query(product_id, True, session)
                    if not ratings_found:
                        ratings_retries += 1
                    elif ratings_retries > 0:
                        logger.info(f'RVQ >>> Successfully retried (verified owner) for {product_id}.')
                
                db_cursor.execute('SELECT gp_title FROM gog_products WHERE gp_id = ?', (product_id,))
                result = db_cursor.fetchone()
                product_title = result[0]
                
                if entry_count == 0:
                    # grt_int_nr, grt_int_added, grt_int_removed, grt_int_updated, grt_int_json_payload, 
                    # grt_int_json_diff, grt_int_id, grt_int_title, grt_review_count, 
                    # grt_avg_rating, grt_avg_rating_count, grt_avg_rating_verified_owner, 
                    # grt_avg_rating_verified_owner_count, grt_is_reviewable
                    db_cursor.execute(INSERT_RATING_QUERY, (None, datetime.now(), None, None, json_formatted, 
                                                            None, product_id, product_title, review_count, 
                                                            avg_rating, avg_rating_count, avg_rating_verified_owner, 
                                                            avg_rating_verified_owner_count, is_reviewable))
                    db_connection.commit()
                    logger.info(f'RVQ +++ Added a new DB entry for {product_id}: {product_title}.')
                
                elif entry_count == 1:
                    db_cursor.execute('SELECT grt_int_removed, grt_int_title, grt_int_json_payload FROM gog_ratings WHERE grt_int_id = ?', (product_id,))
                    existing_removed, existing_product_title, existing_json_formatted = db_cursor.fetchone()
                    
                    # clear the removed status if an id is readded (should only happen rarely)
                    if existing_removed is not None:
                        logger.debug(f'RVQ >>> Found a removed entry with id {product_id}. Clearing removed status...')
                        db_cursor.execute('UPDATE gog_ratings SET grt_int_removed = NULL WHERE grt_int_id = ?', (product_id,))
                        db_connection.commit()
                        logger.info(f'RVQ *** Cleared removed status for {product_id}: {product_title}.')
                    
                    if product_title is not None and existing_product_title != product_title:
                        logger.info(f'RVQ >>> Found a valid (or new) product title: {product_title}. Updating...')
                        db_cursor.execute('UPDATE gog_ratings SET grt_int_title = ? WHERE grt_int_id = ?',
                                              (product_title, product_id))
                        db_connection.commit()
                        logger.info(f'RVQ ~~~ Successfully updated product title for DB entry with id {product_id}.')
                    
                    if existing_json_formatted != json_formatted:
                        logger.debug(f'RVQ >>> Existing entry for {product_id} is outdated. Updating...')
                        
                        # calculate the diff between the new json and the previous one 
                        # (applying the diff on the new json will revert to the previous version)
                        if existing_json_formatted is not None:
                            diff_formatted = ''.join([line for line in difflib.unified_diff(json_formatted.splitlines(1), 
                                                                                            existing_json_formatted.splitlines(1), n=0)])
                        else:
                            diff_formatted = None
                        
                        # grt_int_updated, grt_int_json_payload, grt_int_json_diff, 
                        # grt_review_count, grt_avg_rating, grt_avg_rating_count, grt_avg_rating_verified_owner, 
                        # grt_avg_rating_verified_owner_count, grt_is_reviewable, grt_int_id (WHERE clause)
                        db_cursor.execute(UPDATE_RATING_QUERY, (datetime.now(), json_formatted, diff_formatted, 
                                                                review_count, avg_rating, avg_rating_count, avg_rating_verified_owner, 
                                                                avg_rating_verified_owner_count, is_reviewable, product_id))
                        db_connection.commit()
                        logger.info(f'RVQ ~~~ Updated the DB entry for {product_id}: {product_title}.')
            
            else:
                # existing ids that no longer have any pages are considered removed
                if entry_count == 1:
                    # check to see the existing value for grt_int_removed
                    db_cursor = db_connection.execute('SELECT grt_int_title, grt_int_removed FROM gog_ratings WHERE grt_int_id = ?', (product_id,))
                    product_title, existing_removed = db_cursor.fetchone()
                    
                    # only alter the entry if not already marked as removed
                    if existing_removed is None:
                        logger.debug(f'RVQ >>> Rating for {product_id} has been removed...')
                        # also clear diff field when marking a rating as removed
                        db_cursor.execute('UPDATE gog_ratings SET grt_int_removed = ?, grt_int_json_diff = NULL '
                                            'WHERE grt_int_id = ?', (datetime.now(), product_id))
                        db_connection.commit()
                        logger.info(f'RVQ --- Marked the DB entry for: {product_id}: {product_title} as removed.')
                    else:
                        logger.debug(f'RVQ >>> Rating for {product_id} is already marked as removed.')
                else:
                    logger.debug(f'RVQ >>> {product_id} doesn\'t have any ratings.')
        
        # some ids will return a 504 error - skip them
        elif response.status_code == 504:
            logger.warning(f'RVQ >>> Product with id {product_id} returned a HTTP 504 error code. Skipping.')
        
        else:
            logger.warning(f'RVQ >>> HTTP error code {response.status_code} received for {product_id}.')
            raise Exception()
        
        return True
    
    # sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'RVQ >>> Connection SSL error encountered for {product_id}.')
        return False
    
    # sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'RVQ >>> Connection error encountered for {product_id}.')
        return False
    
    except:
        logger.debug(f'RVQ >>> Reviews query has failed for {product_id}.')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        
        return False

if __name__ == "__main__":
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)
    
    parser = argparse.ArgumentParser(description=('GOG ratings scan (part of gog_gles) - a script to call publicly available GOG APIs '
                                                  'in order to retrieve product rating information.'))
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-u', '--update', help='Perform an update ratings scan, to add/update ratings for existing product IDs', action='store_true')
    group.add_argument('-r', '--removed', help='Perform an removed rating scan, to recheck ratings for any removed product IDs', action='store_true')
    
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
        
    except:
        logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
        raise SystemExit(1)
    
    logger.info('*** Running RATINGS scan script ***')
    
    # detect any parameter overrides and set the scan_mode accordingly
    if len(argv) > 1:
        logger.info('Command-line parameter mode override detected.')
        
        if args.update:
            scan_mode = 'update'
        elif args.archive:
            scan_mode = 'archive'
    
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
    
    terminate_signal = False
    fail_signal = False
    
    if scan_mode == 'update':
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
                db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id > ? AND '
                                                  'gp_int_delisted IS NULL ORDER BY 1', (last_id,))
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all applicable product ids from the DB...')
                
                last_id_counter = 0
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_signal:
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')
                        
                        retries_complete = gog_reviews_query(current_product_id, 
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
                                fail_signal = True
                                terminate_signal = True
                    
                    if last_id_counter % ID_SAVE_FREQUENCY == 0 and not terminate_signal:
                        configParser.read(CONF_FILE_PATH)
                        configParser['UPDATE_SCAN']['last_id'] = str(current_product_id)
                        
                        with open(CONF_FILE_PATH, 'w') as file:
                            configParser.write(file)
                        
                        logger.info(f'Saved scan up to last_id of {current_product_id}.')
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_signal = True
            logger.info('Stopping update scan...')
    
    elif scan_mode == 'removed':
        logger.info('--- Running in REMOVED scan mode ---')
        
        try:
            with sqlite3.connect(DB_FILE_PATH) as db_connection:
                db_cursor = db_connection.execute('SELECT grt_int_id FROM gog_ratings WHERE grt_int_removed IS NOT NULL')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all applicable product ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_signal:
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')
                        
                        retries_complete = gog_reviews_query(current_product_id, 
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
                                fail_signal = True
                                terminate_signal = True
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_signal = True
            logger.info('Stopping removed scan...')
    
    if not terminate_signal and scan_mode == 'update':
        logger.info('Resetting last_id parameter...')
        configParser.read(CONF_FILE_PATH)
        configParser['UPDATE_SCAN']['last_id'] = ''
        
        with open(CONF_FILE_PATH, 'w') as file:
            configParser.write(file)
    
    logger.info('All done! Exiting...')
    
    # return a non-zero exit code if a scan failure was encountered
    if terminate_signal and fail_signal:
        raise SystemExit(4)
