#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 1.60
@date: 23/10/2020

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
INSERT_PRICES_QUERY = 'INSERT INTO gog_prices VALUES (?,?,?,?,?,?,?,?,?,?)'

OPTIMIZE_QUERY = 'PRAGMA optimize'
    
def gog_prices_query(product_id, product_title, country_code, currencies_list, session, db_connection):
    global terminate_signal
    
    prices_url = f'https://api.gog.com/products/{product_id}/prices?countryCode={country_code}'
    
    try:
        response = session.get(prices_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'PRQ >>> HTTP response code: {response.status_code}')
        
        if response.status_code == 200:
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            items = json_parsed['_embedded']['prices']
            logger.debug(f'PRQ >>> Items count: {len(items)}')
            
            if len(items) > 0:
                logger.debug(f'PRQ >>> Found something for id {product_id}...')
            
                db_cursor = db_connection.cursor()
                
                for json_item in items:
                    currency = json_item['currency']['code']
                    logger.debug(f'PRQ >>> currency is: {currency}')
                    
                    if currency in currencies_list or 'all' in currencies_list:
                        #remove currency value from all price values along with any whitespace
                        base_price_str = json_item['basePrice'].replace(currency,'').strip()
                        if base_price_str != '0':
                            base_price = float(base_price_str[:-2] + "." + base_price_str[-2:])
                        else:
                            base_price = 0
                        logger.debug(f'PRQ >>> base_price is: {base_price}')
                        
                        final_price_str = json_item['finalPrice'].replace(currency,'').strip()
                        if final_price_str != '0':
                            final_price = float(final_price_str[:-2] + "." + final_price_str[-2:])
                        else:
                            final_price = 0
                        logger.debug(f'PRQ >>> final_price is: {final_price}')
                        
                        bonus_wallet_funds_str = json_item['bonusWalletFunds'].replace(currency,'').strip()
                        if bonus_wallet_funds_str != '0':
                            bonus_wallet_funds = float(bonus_wallet_funds_str[:-2] + "." + bonus_wallet_funds_str[-2:])
                        #treat 0 value bonus_wallet_funds as None
                        else:
                            bonus_wallet_funds = None
                        logger.debug(f'PRQ >>> bonus_wallet_funds is: {bonus_wallet_funds}')
                        
                        if bonus_wallet_funds is None:
                            db_cursor.execute('SELECT COUNT(gpr_id) FROM gog_prices WHERE gpr_id = ? AND gpr_country_code = ? AND gpr_currency = ? '
                                              'and gpr_base_price = ? AND gpr_final_price = ? AND gpr_bonus_wallet_funds IS NULL AND gpr_int_outdated_on IS NULL',
                                              (product_id, country_code, currency, base_price, final_price))
                        else:
                            db_cursor.execute('SELECT COUNT(gpr_id) FROM gog_prices WHERE gpr_id = ? AND gpr_country_code = ? AND gpr_currency = ? '
                                              'AND gpr_base_price = ? AND gpr_final_price = ? AND gpr_bonus_wallet_funds = ? AND gpr_int_outdated_on IS NULL',
                                              (product_id, country_code, currency, base_price, final_price, bonus_wallet_funds))
                            
                        existing_entries = db_cursor.fetchone()[0]
                        
                        if existing_entries == 0:
                            db_cursor.execute('SELECT count(gpr_id) FROM gog_prices WHERE gpr_id = ? AND gpr_country_code = ? '
                                              'and gpr_currency = ? AND gpr_int_outdated_on IS NULL', (product_id, country_code, currency))
                            previous_entries = db_cursor.fetchone()[0]
                            
                            if previous_entries == 1:
                                db_cursor.execute('UPDATE gog_prices SET gpr_int_outdated_on = ? WHERE gpr_id = ? AND gpr_country_code = ? '
                                                  'AND gpr_currency = ? AND gpr_int_outdated_on IS NULL', (datetime.now(), product_id, country_code, currency))
                                db_connection.commit()
                                logger.debug(f'PRQ ~~~ Succesfully outdated the previous DB entry for {product_id}, {country_code} and {currency} currency')
                            
                            #gpr_int_nr, gpr_int_added, gpr_int_outdated_on, gpr_id, gpr_product_title, gpr_country_code
                            insert_values = [None, datetime.now(), None, product_id, product_title, country_code]
                            #gpr_currency
                            insert_values.append(currency)
                            #gpr_base_price
                            insert_values.append(base_price)
                            #gpr_final_price
                            insert_values.append(final_price)
                            #gpr_bonus_wallet_funds
                            insert_values.append(bonus_wallet_funds)
                        
                            db_cursor.execute(INSERT_PRICES_QUERY, insert_values)
                            db_connection.commit()
                            logger.info(f'PRQ +++ Added a DB entry for {product_id}, {country_code} and {currency} currency')
                        
                        elif existing_entries == 1:
                            logger.debug(f'PRQ >>> Prices have not changed for {product_id}, {country_code} and {currency} currency. Skipping.')
                    
                    else:
                        logger.debug(f'PRQ >>> Currency {currency} is not in currencies_list. Skipping.')
        
        #valid HTTP not found error code, issued for products that are not sold or no longer sold
        elif response.status_code == 400:
            logger.debug(f'PRQ >>> "Bad Request" 400 HTTP error code received for {product_id}.')
        
        elif response.status_code != 200:
            logger.error(f'PRQ >>> HTTP error code received: {response.status_code}.')
            raise Exception()
    
    except:
        logger.error(f'PRQ >>> Processing has failed for {product_id}!')
        raise

##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description=('GOG prices scan (part of gog_visor) - a script to call publicly available GOG APIs '
                                              'in order to retrieve product price information.'))

group = parser.add_mutually_exclusive_group()
group.add_argument('-f', '--full', help='Perform a full price scan, to add add/update prices for existing product IDs', action='store_true')
group.add_argument('-u', '--update', help='Perform an update price scan, to outdate prices for any unlisted product IDs', action='store_true')

args = parser.parse_args()

logger.info('*** Running PRICES scan script ***')
    
try:
    #reading from config file
    configParser.read(conf_file_full_path)
    #parsing generic parameters
    conf_backup = configParser['GENERAL']['conf_backup']
    db_backup = configParser['GENERAL']['db_backup']
    scan_mode = configParser['GENERAL']['scan_mode']
    country_code = configParser['GENERAL']['country_code']
    currencies_list = configParser['GENERAL']['currencies_list']
    currencies_list = [currency.strip() for currency in currencies_list.split(',')]
    #parsing constants
    HTTP_TIMEOUT = int(configParser['GENERAL']['http_timeout'])
except:
    logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
    raise Exception()

#detect any parameter overrides and set the scan_mode accordingly
if len(argv) > 1:
    logger.info('Command-line parameter mode override detected.')
    
    if args.full:
        scan_mode = 'full'
    elif args.update:
        scan_mode = 'update'

if conf_backup == 'true':
    #conf file check/backup section
    if path.exists(conf_file_full_path):
        #create a backup of the existing conf file - mostly for debugging/recovery
        copy2(conf_file_full_path, conf_file_full_path + '.bak')
        logger.info('Successfully created conf file backup.')
    else:
        logger.critical('Could find specified conf file!')
        raise Exception()

if db_backup == 'true':
    #db file check/backup section
    if path.exists(db_file_full_path):
        #create a backup of the existing db - mostly for debugging/recovery
        copy2(db_file_full_path, db_file_full_path + '.bak')
        logger.info('Successfully created db backup.')
    else:
        #subprocess.run(['python', 'gog_create_db.py'])
        logger.critical('Could find specified DB file!')
        raise Exception()

if scan_mode == 'full':
    logger.info('--- Running in FULL scan mode ---')
    
    last_id = int(configParser['FULL_SCAN']['last_id'])
    id_save_interval = int(configParser['FULL_SCAN']['id_save_interval'])
    
    if last_id > 0:
        logger.info(f'Restarting full scan from id: {last_id}')
    
    try:
        logger.info('Starting full scan on all applicable DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            db_cursor = db_connection.execute('SELECT gp_id, gp_title FROM gog_products WHERE gp_id > ? ORDER BY 1', (last_id, ))
                
            array_of_id_lists = db_cursor.fetchall()
            logger.debug('Retrieved all applicable product ids from the DB...')
        
            #used to track the number of processed ids
            last_id_counter = 0
            
            with requests.Session() as session:
                for id_list in array_of_id_lists:
                    current_product_id = id_list[0]
                    current_product_title = id_list[1]
                    logger.debug(f'Now processing id {current_product_id}...')
                    complete = False
                    retry_counter = 0
                    
                    while not complete:
                        #will only enter in case of 509 HTTP errors, which are quite common due to GOG's throttling system
                        if retry_counter > 0:
                            logger.warning(f'Reprocessing id {current_product_id}...')
                            #allow a short respite before re-processing
                            sleep(2)
                        try:
                            gog_prices_query(current_product_id, current_product_title, country_code, currencies_list, session, db_connection)
                            complete = True
                            
                            if last_id_counter != 0 and last_id_counter % id_save_interval == 0:
                                configParser.read(conf_file_full_path)
                                configParser['FULL_SCAN']['last_id'] = str(current_product_id)
                                
                                with open(conf_file_full_path, 'w') as file:
                                    configParser.write(file)
                                    
                                logger.info(f'Saved scan up to last_id of: {current_product_id}')
                            
                            last_id_counter += 1
                                
                        except KeyboardInterrupt:
                            raise
                        except:
                            complete = False
                            retry_counter += 1
                            #uncomment for debugging purposes only
                            #raise
            
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        reset_id = False
    
elif scan_mode == 'update':
    logger.info('--- Running in UPDATE scan mode ---')
    
    try:
        logger.info('Starting update scan on all applicable DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            db_cursor = db_connection.execute('SELECT gpr_id FROM gog_prices WHERE gpr_id NOT IN (SELECT gp_id FROM gog_products ORDER BY 1) ORDER BY 1')
            array_of_id_lists = db_cursor.fetchall()
            logger.debug('Retrieved all applicable product ids from the DB...')
            
            for id_list in array_of_id_lists:
                current_product_id = id_list[0]
                
                db_cursor.execute('UPDATE gog_prices SET gpr_int_outdated_on = ? WHERE gpr_id = ? AND gpr_country_code = ? '
                                  'AND gpr_int_outdated_on IS NULL', (datetime.now(), current_product_id, country_code))
                db_connection.commit()
                logger.info(f'Succesfully outdated the DB entry for {current_product_id}, {country_code} and all currencies')
                
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
    
    except KeyboardInterrupt:
        pass

#if nothing went wrong, reset the last_id parameter to 0
if scan_mode == 'full' and reset_id == True:
    logger.info('Resetting last_id parameter...')
    configParser.read(conf_file_full_path)
    configParser['FULL_SCAN']['last_id'] = '0'
            
    with open(conf_file_full_path, 'w') as file:
        configParser.write(file)

logger.info('All done! Exiting...')
        
##main thread end
