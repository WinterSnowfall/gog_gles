#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 4.02
@date: 10/12/2023

Warning: Built for use with python 3.6+
'''

import json
import sqlite3
import signal
import requests
import logging
import argparse
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
CONF_FILE_PATH = os.path.join('..', 'conf', 'gog_prices_scan.conf')

# logging configuration block
LOG_FILE_PATH = os.path.join('..', 'logs', 'gog_prices_scan.log')
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
INSERT_PRICES_QUERY = 'INSERT INTO gog_prices VALUES (?,?,?,?,?,?,?,?,?)'

OPTIMIZE_QUERY = 'PRAGMA optimize'

HTTP_OK = 200

def sigterm_handler(signum, frame):
    logger.debug('Stopping scan due to SIGTERM...')

    raise SystemExit(0)

def sigint_handler(signum, frame):
    logger.debug('Stopping scan due to SIGINT...')

    raise SystemExit(0)

def gog_prices_query(product_id, country_code, currencies_list, session, db_connection):

    prices_url = f'https://api.gog.com/products/{product_id}/prices?countryCode={country_code}'

    try:
        response = session.get(prices_url, timeout=HTTP_TIMEOUT)

        logger.debug(f'PQ >>> HTTP response code: {response.status_code}.')

        if response.status_code == HTTP_OK:
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)

            items = json_parsed['_embedded']['prices']
            logger.debug(f'PQ >>> Items count: {len(items)}.')

            if len(items) > 0:
                logger.debug(f'PQ >>> Found something for id {product_id}...')

                db_cursor = db_connection.execute('SELECT gp_title FROM gog_products WHERE gp_id = ?', (product_id,))
                result = db_cursor.fetchone()
                product_title = result[0]

                for json_item in items:
                    currency = json_item['currency']['code']
                    logger.debug(f'PQ >>> currency is: {currency}.')

                    if currency in currencies_list or 'all' in currencies_list:
                        # remove currency value from all price values along with any whitespace
                        base_price_str = json_item['basePrice'].replace(currency, '').strip()
                        if base_price_str != '0':
                            base_price = float(''.join((base_price_str[:-2], '.', base_price_str[-2:])))
                        else:
                            base_price = 0
                        logger.debug(f'PQ >>> base_price is: {base_price}.')

                        final_price_str = json_item['finalPrice'].replace(currency, '').strip()
                        if final_price_str != '0':
                            final_price = float(''.join((final_price_str[:-2], '.', final_price_str[-2:])))
                        else:
                            final_price = 0
                        logger.debug(f'PQ >>> final_price is: {final_price}.')

                        db_cursor.execute('SELECT COUNT(*) FROM gog_prices WHERE gpr_int_id = ? AND gpr_int_outdated IS NULL '
                                          'AND gpr_int_country_code = ? AND gpr_currency = ? AND gpr_base_price = ? AND gpr_final_price = ?',
                                          (product_id, country_code, currency, base_price, final_price))
                        existing_entries = db_cursor.fetchone()[0]

                        if existing_entries == 0:
                            db_cursor.execute('SELECT COUNT(*) FROM gog_prices WHERE gpr_int_id = ? AND gpr_int_outdated IS NULL '
                                              'AND gpr_int_country_code = ? AND gpr_currency = ?', (product_id, country_code, currency))
                            previous_entries = db_cursor.fetchone()[0]

                            if previous_entries == 1:
                                db_cursor.execute('UPDATE gog_prices SET gpr_int_outdated = ? WHERE gpr_int_id = ? AND gpr_int_outdated IS NULL '
                                                  'AND gpr_int_country_code = ? AND gpr_currency = ?', (datetime.now(), product_id, country_code, currency))
                                db_connection.commit()
                                logger.debug(f'PQ ~~~ Succesfully outdated the previous DB entry for {product_id}: {product_title}, {country_code}, {currency}.')

                            # gpr_int_nr, gpr_int_added, gpr_int_outdated, gpr_int_id, gpr_int_title,
                            # gpr_int_country_code, gpr_currency, gpr_base_price, gpr_final_price
                            db_cursor.execute(INSERT_PRICES_QUERY, (None, datetime.now(), None, product_id, product_title,
                                                                    country_code, currency, base_price, final_price))
                            db_connection.commit()
                            logger.info(f'PQ +++ Added a DB entry for {product_id}: {product_title}, {country_code}, {currency}.')

                        elif existing_entries == 1:
                            logger.debug(f'PQ >>> Prices have not changed for {product_id}, {country_code}, {currency}. Skipping.')

                    else:
                        logger.debug(f'PQ >>> {currency} is not in currencies_list. Skipping.')

        # HTTP error code 400, issued for products that are not sold or no longer sold
        elif response.status_code == 400:
            logger.debug(f'PQ >>> HTTP error code 400 (Bad Request) received for {product_id}.')

        else:
            logger.warning(f'PQ >>> HTTP error code {response.status_code} received for {product_id}.')
            raise Exception()

        return True

    # sometimes the connection may time out
    except requests.Timeout:
        logger.warning(f'PQ >>> HTTP request timed out after {HTTP_TIMEOUT} seconds.')
        return False

    # sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'PQ >>> Connection SSL error encountered for {product_id}.')
        return False

    # sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'PQ >>> Connection error encountered for {product_id}.')
        return False

    except:
        logger.debug(f'PQ >>> Prices query has failed for {product_id}, {country_code}, {currency}.')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)

    parser = argparse.ArgumentParser(description=('GOG prices scan (part of gog_gles) - a script to call publicly available GOG APIs '
                                                  'in order to retrieve product price information.'))

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-u', '--update', help='Perform an update price scan, to add/update prices for existing product IDs', action='store_true')
    group.add_argument('-a', '--archive', help='Perform an archive price scan, to outdate prices for any delisted product IDs', action='store_true')

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
        COUNTRY_CODE = general_section.get('country_code')
        CURRENCIES_LIST = [currency.strip() for currency in general_section.get('currencies_list').split(',')]
        HTTP_TIMEOUT = general_section.getint('http_timeout')
        RETRY_COUNT = general_section.getint('retry_count')
        RETRY_SLEEP_INTERVAL = general_section.getint('retry_sleep_interval')
    except:
        logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
        raise SystemExit(1)

    logger.info('*** Running PRICES scan script ***')

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

                        retries_complete = gog_prices_query(current_product_id, COUNTRY_CODE, CURRENCIES_LIST,
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

    elif scan_mode == 'archive':
        logger.info('--- Running in ARCHIVE scan mode ---')

        try:
            with sqlite3.connect(DB_FILE_PATH) as db_connection:
                db_cursor = db_connection.execute('SELECT DISTINCT gpr_int_id, gpr_int_title FROM gog_prices WHERE gpr_int_outdated IS NULL '
                                                  'AND gpr_int_id IN (SELECT gp_id FROM gog_products WHERE gp_int_delisted IS NOT NULL '
                                                  'ORDER BY 1) ORDER BY 1')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all applicable product ids from the DB...')

                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    current_product_title = id_entry[1]
                    logger.debug(f'Now processing id {current_product_id}...')

                    db_cursor.execute('UPDATE gog_prices SET gpr_int_outdated = ? WHERE gpr_int_id = ? AND gpr_int_outdated IS NULL '
                                      'AND gpr_int_country_code = ?', (datetime.now(), current_product_id, COUNTRY_CODE))
                    logger.info(f'Succesfully outdated the DB entry for {current_product_id}: {current_product_title}, {COUNTRY_CODE}, all currencies.')

                db_connection.commit()

                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)

        except SystemExit:
            terminate_signal = True
            logger.info('Stopping archive scan...')

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
