#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 2.00
@date: 22/11/2020

Warning: Built for use with python 3.6+
'''

import json
import sqlite3
import requests
import logging
import argparse
from sys import argv
from shutil import copy2
from configparser import ConfigParser
from datetime import datetime
from os import path
from time import sleep
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
#uncomment for debugging purposes only
#import traceback

##global parameters init
configParser = ConfigParser()
terminate_signal = False
reset_id = True

##conf file block
conf_file_full_path = path.join('..', 'conf', 'gog_prices_scan.conf')

##logging configuration block
log_file_full_path = path.join('..', 'logs', 'gog_prices_scan.log')
logger_file_handler = RotatingFileHandler(log_file_full_path, maxBytes=8388608, backupCount=1, encoding='utf-8')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler.setFormatter(logging.Formatter(logger_format))
logging.basicConfig(format=logger_format, level=logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
logger.addHandler(logger_file_handler)

##db configuration block
db_file_full_path = path.join('..', 'output_db', 'gog_visor.db')

##CONSTANTS
INSERT_PRICES_QUERY = 'INSERT INTO gog_prices VALUES (?,?,?,?,?,?,?,?,?)'

OPTIMIZE_QUERY = 'PRAGMA optimize'
    
def gog_prices_query(product_id, product_title, country_code, currencies_list, session, db_connection):
    global terminate_signal
    
    prices_url = f'https://api.gog.com/products/{product_id}/prices?countryCode={country_code}'
    
    try:
        response = session.get(prices_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'PQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == 200:
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            items = json_parsed['_embedded']['prices']
            logger.debug(f'PQ >>> Items count: {len(items)}.')
            
            if len(items) > 0:
                logger.debug(f'PQ >>> Found something for id {product_id}...')
            
                db_cursor = db_connection.cursor()
                
                for json_item in items:
                    currency = json_item['currency']['code']
                    logger.debug(f'PQ >>> currency is: {currency}.')
                    
                    if currency in currencies_list or 'all' in currencies_list:
                        #remove currency value from all price values along with any whitespace
                        base_price_str = json_item['basePrice'].replace(currency,'').strip()
                        if base_price_str != '0':
                            base_price = float(base_price_str[:-2] + "." + base_price_str[-2:])
                        else:
                            base_price = 0
                        logger.debug(f'PQ >>> base_price is: {base_price}.')
                        
                        final_price_str = json_item['finalPrice'].replace(currency,'').strip()
                        if final_price_str != '0':
                            final_price = float(final_price_str[:-2] + "." + final_price_str[-2:])
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
                            
                            #gpr_int_nr, gpr_int_added, gpr_int_outdated, gpr_int_id, gpr_int_title, 
                            #gpr_int_country_code, gpr_currency, gpr_base_price, gpr_final_price
                            db_cursor.execute(INSERT_PRICES_QUERY, (None, datetime.now(), None, product_id, product_title, 
                                                                    country_code, currency, base_price, final_price))
                            db_connection.commit()
                            logger.info(f'PQ +++ Added a DB entry for {product_id}: {product_title}, {country_code}, {currency}.')
                        
                        elif existing_entries == 1:
                            logger.debug(f'PQ >>> Prices have not changed for {product_id}, {country_code}, {currency}. Skipping.')
                    
                    else:
                        logger.debug(f'PQ >>> {currency} is not in currencies_list. Skipping.')
        
        #HTTP 400 error code, issued for products that are not sold or no longer sold
        elif response.status_code == 400:
            logger.debug(f'PQ >>> "Bad Request" 400 HTTP error code received for {product_id}.')
        
        else:
            logger.warning(f'PQ >>> HTTP error code {response.status_code} received for {product_id}.')
            raise Exception()
        
        return True
    
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'PQ >>> Connection error encountered for {product_id}.')
        
        return False
    
    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'PQ >>> Connection SSL error encountered for {product_id}.')
        
        return False
    
    except:
        logger.debug(f'PQ >>> Prices query has failed for {product_id}, {country_code}, {currency}.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        
        return False

##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description=('GOG prices scan (part of gog_visor) - a script to call publicly available GOG APIs '
                                              'in order to retrieve product price information.'))

group = parser.add_mutually_exclusive_group()
group.add_argument('-u', '--update', help='Perform an update price scan, to add/update prices for existing product IDs', action='store_true')
group.add_argument('-a', '--archive', help='Perform an archive price scan, to outdate prices for any delisted product IDs', action='store_true')

args = parser.parse_args()

logger.info('*** Running PRICES scan script ***')
    
try:
    #reading from config file
    configParser.read(conf_file_full_path)
    general_section = configParser['GENERAL']
    #parsing generic parameters
    conf_backup = general_section.getboolean('conf_backup')
    db_backup = general_section.getboolean('db_backup')
    scan_mode = general_section.get('scan_mode')
    country_code = general_section.get('country_code')
    currencies_list = general_section.get('currencies_list')
    currencies_list = [currency.strip() for currency in currencies_list.split(',')]
    #parsing constants
    HTTP_TIMEOUT = general_section.getint('http_timeout')
except:
    logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
    raise SystemExit(1)

#detect any parameter overrides and set the scan_mode accordingly
if len(argv) > 1:
    logger.info('Command-line parameter mode override detected.')
    
    if args.update:
        scan_mode = 'update'
    elif args.archive:
        scan_mode = 'archive'

if conf_backup:
    #conf file check/backup section
    if path.exists(conf_file_full_path):
        #create a backup of the existing conf file - mostly for debugging/recovery
        copy2(conf_file_full_path, conf_file_full_path + '.bak')
        logger.info('Successfully created conf file backup.')
    else:
        logger.critical('Could find specified conf file!')
        raise SystemExit(2)

if db_backup:
    #db file check/backup section
    if path.exists(db_file_full_path):
        #create a backup of the existing db - mostly for debugging/recovery
        copy2(db_file_full_path, db_file_full_path + '.bak')
        logger.info('Successfully created db backup.')
    else:
        #subprocess.run(['python', 'gog_create_db.py'])
        logger.critical('Could find specified DB file!')
        raise SystemExit(3)

if scan_mode == 'update':
    logger.info('--- Running in UPDATE scan mode ---')
    
    update_scan_section = configParser['UPDATE_SCAN']
    #product_id will restart from scan_id
    ID_SAVE_INTERVAL = update_scan_section.getint('id_save_interval')
    last_id = update_scan_section.getint('last_id')
    
    if last_id > 0:
        logger.info(f'Restarting full scan from id: {last_id}.')
    
    try:
        logger.info('Starting full scan on all applicable DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            db_cursor = db_connection.execute('SELECT gp_id, gp_title FROM gog_products WHERE gp_id > ? ORDER BY 1', (last_id, ))
            id_list = db_cursor.fetchall()
            logger.debug('Retrieved all applicable product ids from the DB...')
        
            #used to track the number of processed ids
            last_id_counter = 0
            
            with requests.Session() as session:
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    current_product_title = id_entry[1]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete:
                        if retry_counter > 0:
                            logger.warning(f'Reprocessing id {current_product_id}...')
                            #allow a short respite before re-processing
                            sleep(2)
                            
                        retries_complete = gog_prices_query(current_product_id, current_product_title, country_code, currencies_list, session, db_connection)
                            
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                                
                            last_id_counter += 1
                                
                        else:
                            retry_counter += 1
                                
                    if last_id_counter != 0 and last_id_counter % ID_SAVE_INTERVAL == 0:
                        configParser.read(conf_file_full_path)
                        configParser['FULL_SCAN']['last_id'] = str(current_product_id)
                        
                        with open(conf_file_full_path, 'w') as file:
                            configParser.write(file)
                            
                        logger.info(f'Saved scan up to last_id of {current_product_id}.')
            
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        reset_id = False
    
elif scan_mode == 'archive':
    logger.info('--- Running in ARCHIVE scan mode ---')
    
    try:
        logger.info('Starting update scan on all applicable DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            db_cursor = db_connection.execute('SELECT DISTINCT gpr_int_id, gpr_int_title FROM gog_prices WHERE gpr_int_outdated IS NULL '
                                              'AND gpr_int_id NOT IN (SELECT gp_id FROM gog_products ORDER BY 1) ORDER BY 1')
            id_list = db_cursor.fetchall()
            logger.debug('Retrieved all applicable product ids from the DB...')
            
            for id_entry in id_list:
                current_product_id = id_entry[0]
                current_product_title = id_entry[1]
                logger.debug(f'Now processing id {current_product_id}...')
                
                db_cursor.execute('UPDATE gog_prices SET gpr_int_outdated = ? WHERE gpr_int_id = ? AND gpr_int_outdated IS NULL '
                                  'AND gpr_int_country_code = ?', (datetime.now(), current_product_id, country_code))
                logger.info(f'Succesfully outdated the DB entry for {current_product_id}: {current_product_title}, {country_code}, all currencies.')
                
            #batch commit
            db_connection.commit()
                
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
    
    except KeyboardInterrupt:
        pass

#if nothing went wrong, reset the last_id parameter to 0
if scan_mode == 'update' and reset_id:
    logger.info('Resetting last_id parameter...')
    configParser.read(conf_file_full_path)
    configParser['FULL_SCAN']['last_id'] = '0'
            
    with open(conf_file_full_path, 'w') as file:
        configParser.write(file)

logger.info('All done! Exiting...')
        
##main thread end
