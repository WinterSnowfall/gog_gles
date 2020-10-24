#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 1.60
@date: 23/10/2020

Warning: Built for use with python 3.6+
'''

import json
import html
import sqlite3
import requests
import logging
import argparse
from sys import argv
from shutil import copy2
from configparser import ConfigParser
from os import path
from datetime import datetime
from collections import OrderedDict
from lxml import html as lhtml
from logging.handlers import RotatingFileHandler

##global parameters init
configParser = ConfigParser()

##conf file block
conf_file_full_path = path.join('..', 'conf', 'gog_company_scan.conf')

##logging configuration block
log_file_full_path = path.join('..', 'logs', 'gog_company_scan.log')
logger_file_handler = RotatingFileHandler(log_file_full_path, maxBytes=8388608, backupCount=1, encoding='utf-8')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler.setFormatter(logging.Formatter(logger_format))
logging.basicConfig(format=logger_format, level=logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
logger.addHandler(logger_file_handler)

##db configuration block
db_file_full_path = path.join('..', 'output_db', 'gog_visor.db')

##CONSTANTS
OPTIMIZE_QUERY = 'PRAGMA optimize'

#set the gog_lc cookie to avoid errors bought about by GOG dynamically determining the site language
COOKIES = {
    'gog_lc': 'BE_EUR_en-US'
}

def gog_company_query(scan_mode):
    
    company_url = 'https://www.gog.com/games'
    
    try:
        with requests.Session() as session:
            response = session.get(company_url, cookies=COOKIES, timeout=HTTP_TIMEOUT)
            
            logger.debug(f'CQ >>> HTTP response code is: {response.status_code}')
            
            if response.status_code == 200:
                logger.info('CQ >>> Company query has returned a valid response...')
                
                html_tree = lhtml.fromstring(response.text)
                gogData_container_html = html_tree.xpath('//body/script[@type="text/javascript" and contains(./text(), "var gogData = ")]/text()')[0].strip()
                gogData_html = gogData_container_html[gogData_container_html.find('var gogData = ') + 14:
                                                      #remove 10 chars to adjust for extra spacing and endline
                                                      gogData_container_html.find('var translationData = ') - 10]
                gogData_pretty = json.dumps(gogData_html, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                logger.debug(f'CQ >>> gogData value: {gogData_pretty}')
                
                gogData_json = json.loads(gogData_html, object_pairs_hook=OrderedDict)
                
                company_list_pretty = []
                
                with sqlite3.connect(db_file_full_path) as db_connection:
                    db_cursor = db_connection.cursor()
                    
                    for element_tag in gogData_json['catalogFilters']:
                        element_value = element_tag['title']
                        logger.debug(f'CQ >>> Found a title entry with value: {element_value}')
                        
                        if element_value == 'Company':
                            for choice_element in element_tag['choices']:
                                logger.debug('CQ >>> Parsing a new company entry...')
                                #strip any trailing or leading whitespace
                                company_raw_name = choice_element['title'].strip()
                                logger.debug(f'CQ >>> company_raw_name value: {company_raw_name}')
                                #unescape any potentially remanent HTML notations such as '&amp;'
                                company_name = html.unescape(company_raw_name)
                                logger.debug(f'CQ >>> company_name value: {company_name}')
                                #workaround for a miss-match on 'Lion's Shade' caused by a leaning quote ('`') character
                                if company_name.find('`') != -1:
                                    company_name = company_name.replace('`', '\'')
                                #set this to debug in order to highlight new companies only
                                logger.debug(f'CQ >>> Processing company: {company_name}')
                                
                                if scan_mode == 'full':
                                    db_cursor.execute('SELECT COUNT(*) FROM gog_companies WHERE gc_name = ?', (company_name, ))
                                    entry_count = db_cursor.fetchone()[0]
                                    
                                    if entry_count == 0:
                                        logger.info('CQ >>> Detected a new company entry...')
                                        #gc_int_nr, gc_int_added, gc_int_no_longer_listed, gc_name
                                        db_cursor.execute('INSERT INTO gog_companies VALUES (?,?,?,?)', 
                                                    (None, datetime.now(), None, company_name))
                                        db_connection.commit()
                                        
                                        logger.info(f'CQ +++ Added a new DB entry for: {company_name}')
                                    else:
                                        logger.debug(f'CQ >>> Company {company_name} already has a DB entry. Skipping...')
                            
                                elif scan_mode == 'update':
                                    company_list_pretty.append(company_name)
                                    logger.debug(f'CQ >>> Added company to update list: {company_name}')
                    
                    if scan_mode == 'update':
                        db_cursor.execute('SELECT gc_name FROM gog_companies ORDER BY 1')
                        company_names = db_cursor.fetchall()
                        
                        for company_row in company_names:
                            company = company_row[0]
                            logger.debug(f'CQ >>> Now processing company {company}...')
                            
                            if company not in company_list_pretty:
                                db_cursor.execute('SELECT gc_int_no_longer_listed FROM gog_companies WHERE gc_name = ?', (company, ))
                                no_longer_listed = db_cursor.fetchone()[0]
                                
                                if no_longer_listed is None or no_longer_listed == '':
                                    logger.warning(f'CQ >>> Company {company} is no longer listed...')
                                    
                                    db_cursor.execute('UPDATE gog_companies SET gc_int_no_longer_listed = ? WHERE gc_name = ?', (datetime.now(), company))
                                    db_connection.commit()
                                    
                                    logger.info(f'CQ --- Updated the DB entry for: {company}')
                                else:
                                    logger.debug(f'CQ >>> Company {company} is already de-listed. Skipping.')
                    
                    logger.debug('Running PRAGMA optimize...')
                    db_connection.execute(OPTIMIZE_QUERY)
            
            #response.status_code != 200
            else:
                logger.error(f'CQ >>> HTTP error code received: {response.status_code}. Aborting!')
                raise Exception()
    
    except:
        logger.error('CQ >>> Exception encountered. Aborting!')
        #uncomment for debugging purposes only
        #raise

##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description=('GOG company scan (part of gog_visor) - a script to call publicly available GOG APIs '
                                              'in order to retrieve company information.'))

group = parser.add_mutually_exclusive_group()
group.add_argument('-f', '--full', help='Perform a full company scan', action='store_true')
group.add_argument('-u', '--update', help='Perform an update company scan', action='store_true')

args = parser.parse_args()

logger.info('*** Running COMPANY scan script ***')
    
try:
    #reading from config file
    configParser.read(conf_file_full_path)
    #parsing generic parameters
    db_backup = configParser['GENERAL']['db_backup']
    scan_mode = configParser['GENERAL']['scan_mode']
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

#allow scan_mode specific db backup strategies
if db_backup == 'true' or db_backup == scan_mode:
    #db file check/backup section
    if path.exists(db_file_full_path):
        #create a backup of the existing db - mostly for debugging/recovery
        copy2(db_file_full_path, db_file_full_path + '.bak')
        logger.info('Successfully created DB backup.')
    else:
        #subprocess.run(['python', 'gog_create_db.py']) 
        logger.critical('Could find specified DB file!')
        raise Exception()

if scan_mode == 'full':
    logger.info('--- Running in FULL scan mode ---')
    gog_company_query(scan_mode)

elif scan_mode == 'update':
    logger.info('--- Running in UPDATE scan mode ---')
    gog_company_query(scan_mode)
    
logger.info('All done! Exiting...')

##main thread end
