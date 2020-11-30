#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 2.00
@date: 22/11/2020

Warning: Built for use with python 3.6+
'''

import json
import html
import sqlite3
import requests
import logging
import argparse
from shutil import copy2
from configparser import ConfigParser
from os import path
from datetime import datetime
from collections import OrderedDict
from lxml import html as lhtml
from logging.handlers import RotatingFileHandler
#uncomment for debugging purposes only
#import traceback

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

GOGDATA_START_OFFSET = 14
GOGDATA_END_OFFSET = 10

COMPANY_URL = 'https://www.gog.com/games'

#set the gog_lc cookie to avoid errors bought about by GOG dynamically determining the site language
COOKIES = {
    'gog_lc': 'BE_EUR_en-US'
}

def gog_company_query(company_url):
    
    logger.debug(f'CQ >>> Querying url: {company_url}.')
    
    try:
        with requests.Session() as session:
            response = session.get(company_url, cookies=COOKIES, timeout=HTTP_TIMEOUT)
            
            logger.debug(f'CQ >>> HTTP response code is: {response.status_code}.')
            
            if response.status_code == 200:
                logger.info('CQ >>> Company query has returned a valid response...')
                
                html_tree = lhtml.fromstring(response.text)
                gogData_container_html = html_tree.xpath('//body/script[@type="text/javascript" and contains(./text(), "var gogData = ")]/text()')[0].strip()
                gogData_html = gogData_container_html[gogData_container_html.find('var gogData = ') + GOGDATA_START_OFFSET:
                                                      #remove some chars to adjust for extra spacing and endline
                                                      gogData_container_html.find('var translationData = ') - GOGDATA_END_OFFSET]
                logger.debug(f'CQ >>> gogData value: {gogData_html}')
                gogData_json = json.loads(gogData_html, object_pairs_hook=OrderedDict)
                
                #use a set to avoid processing potentially duplicate ids
                company_values = set()
                
                with sqlite3.connect(db_file_full_path) as db_connection:
                    db_cursor = db_connection.cursor()
                    
                    for element_tag in gogData_json['catalogFilters']:
                        element_value = element_tag['title']
                        logger.debug(f'CQ >>> Found a title entry with value: {element_value}.')
                        
                        if element_value == 'Company':
                            for choice_element in element_tag['choices']:
                                logger.debug('CQ >>> Parsing a new company entry...')
                                #unescape any potentially remanent HTML notations such as '&amp;'
                                company_name = html.unescape(choice_element['title'].strip())
                                logger.debug(f'CQ >>> Processing company: {company_name}.')
                                #add the company_name to the company_values set 
                                #for cross-checking the validity of existing entries
                                company_values.add(company_name)
                                logger.debug(f'CQ >>> Added company to validation set: {company_name}.')
                                
                                db_cursor.execute('SELECT COUNT(*) FROM gog_companies WHERE gc_name = ?', (company_name, ))
                                entry_count = db_cursor.fetchone()[0]
                                
                                if entry_count == 0:
                                    logger.info('CQ >>> Detected a new company entry...')
                                    #gc_int_nr, gc_int_added, gc_int_delisted, gc_name
                                    db_cursor.execute('INSERT INTO gog_companies VALUES (?,?,?,?)', 
                                                      (None, datetime.now(), None, company_name))
                                    db_connection.commit()
                                    
                                    logger.info(f'CQ +++ Added a new DB entry for: {company_name}.')
                                else:
                                    logger.debug(f'CQ >>> Company {company_name} already has a DB entry. Skipping...')
                    
                    #cross-check the validity of existing entries
                    db_cursor.execute('SELECT gc_name, gc_int_delisted FROM gog_companies ORDER BY 1')
                    company_list = db_cursor.fetchall()
                    
                    for company_row in company_list:
                        company = company_row[0]
                        existing_delisted = company_row[1]
                        logger.debug(f'CQ >>> Now validating company {company}...')
                        
                        #delist companies which are no longer in the scraped company_values list
                        if company not in company_values:
                            if existing_delisted is None:
                                logger.warning(f'CQ >>> Company {company} has been delisted...')
                                db_cursor.execute('UPDATE gog_companies SET gc_int_delisted = ? WHERE gc_name = ?', (datetime.now(), company))
                                db_connection.commit()
                                logger.info(f'CQ --- Updated the DB entry for: {company}.')
                        #relist delisted companies if they show up in the scraped company_values list
                        else:
                            if existing_delisted is not None:
                                logger.warning(f'CQ >>> Company {company} has been relisted...')
                                db_cursor.execute('UPDATE gog_companies SET gc_int_delisted = NULL WHERE gc_name = ?', (company, ))
                                db_connection.commit()
                                logger.info(f'CQ ~~~ Updated the DB entry for: {company}.')
                    
                    logger.debug('Running PRAGMA optimize...')
                    db_connection.execute(OPTIMIZE_QUERY)
            
            else:
                logger.error(f'CQ >>> HTTP error code {response.status_code} received. Aborting!')
                raise Exception()
            
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.critical(f'CQ >>> Connection error encountered. Aborting!')
    
    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.critical(f'CQ >>> Connection SSL error encountered. Aborting!')
    
    except:
        logger.critical('CQ >>> Company query has failed. Aborting!')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())

##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description=('GOG company scan (part of gog_visor) - a script to scrape the GOG website '
                                              'in order to retrieve company information.'))

args = parser.parse_args()

logger.info('*** Running COMPANY scan script ***')
    
try:
    #reading from config file
    configParser.read(conf_file_full_path)
    general_section = configParser['GENERAL']
    #parsing generic parameters
    db_backup = general_section.getboolean('db_backup')
    #parsing constants
    HTTP_TIMEOUT = general_section.getint('http_timeout')
except:
    logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
    raise SystemExit(1)

if db_backup:
    #db file check/backup section
    if path.exists(db_file_full_path):
        #create a backup of the existing db - mostly for debugging/recovery
        copy2(db_file_full_path, db_file_full_path + '.bak')
        logger.info('Successfully created DB backup.')
    else:
        logger.critical('Could find DB file in specified path!')
        raise SystemExit(2)

gog_company_query(COMPANY_URL)
    
logger.info('All done! Exiting...')

##main thread end
