#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 1.00
@date: 29/04/2019

Warning: Built for use with python 3.6+
'''

import json
import html
import sqlite3
import requests
import logging
from logging.handlers import RotatingFileHandler
from sys import argv
from shutil import copy2
from configparser import ConfigParser
from os import path
from datetime import datetime
from collections import OrderedDict
from lxml import html as lhtml

##global parameters init
configParser = ConfigParser()

##conf file block
conf_file_full_path = path.join('..', 'conf', 'gog_company_scan.conf')

##logging configuration block
log_file_full_path = path.join('..', 'logs', 'gog_company_scan.log')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler = RotatingFileHandler(log_file_full_path, maxBytes=33554432, backupCount=2, encoding='utf-8')
logger_file_formatter = logging.Formatter(logger_format)
logger_file_handler.setFormatter(logger_file_formatter)
logging.basicConfig(format=logger_format, level=logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
logger.addHandler(logger_file_handler)

##db configuration block
db_file_full_path = path.join('..', 'output_db', 'gog_visor.db')

##CONSTANTS
#set the gog_lc cookie to avoid errors bought about by GOG dynamically determining the site language
COOKIES = {
    'gog_lc': 'BE_EUR_en-US'
}

def gog_company_query(scan_mode):
    
    company_url = 'https://www.gog.com/games'
    
    try:
        with requests.Session() as session:
            #reuse session connection(s) to send a GET request
            response = session.get(company_url, cookies=COOKIES, timeout=300)
            
            logger.debug(f'CQ >>> HTTP response code is: {response.status_code}')
            
            if response.status_code == 200 and response.text != None and response.text.find('"error": "server_error"') == -1:
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
                                if(company_name.find('`') != -1):
                                    company_name = company_name.replace('`', "'")
                                #set this to debug in order to highlight new companies only
                                logger.debug(f'CQ >>> Processing company: {company_name}')
                                
                                if scan_mode == 'full':
                                    cursor = db_connection.cursor()
                                    cursor.execute('SELECT COUNT(*) FROM gog_companies WHERE gc_name = ?', [company_name, ])
                                    entry_count = cursor.fetchone()[0]
                                    
                                    if entry_count == 0:
                                        logger.info('CQ >>> Detected a new company entry...')
                                        
                                        cursor.execute('INSERT INTO gog_companies VALUES(?,?,?,?)', 
                                                    [None,
                                                     str(datetime.now()),
                                                     None,
                                                     company_name])
                                        db_connection.commit()
                                        
                                        logger.info(f'CQ +++ Added a new DB entry for: {company_name}')
                                    else:
                                        logger.debug(f'CQ >>> Company {company_name} already has a DB entry. Skipping...')
                            
                                elif scan_mode == 'update':
                                    company_list_pretty.append(company_name)
                                    logger.debug(f'CQ >>> Added company to update list: {company_name}')
                    
                    if scan_mode == 'update':
                        cursor = db_connection.cursor()
                        cursor.execute('SELECT gc_name FROM gog_companies ORDER BY 1')
                        company_names = cursor.fetchall()
                        
                        for company_row in company_names:
                            company = company_row[0]
                            logger.debug(f'CQ >>> Now processing company {company}...')
                            
                            if company not in company_list_pretty:
                                cursor.execute('SELECT gc_int_no_longer_listed FROM gog_companies where gc_name = ?',
                                               [company, ])
                                no_longer_listed = cursor.fetchone()[0]
                                
                                if no_longer_listed is None or no_longer_listed == '':
                                    logger.warning(f'CQ >>> Company {company} is no longer listed...')
                                    cursor.execute('UPDATE gog_companies SET gc_int_no_longer_listed = ? WHERE gc_name = ?', 
                                                   [str(datetime.now()), company])
                                    db_connection.commit()
                                    logger.info(f'CQ --- Updated the DB entry for: {company}')
                                else:
                                    logger.debug(f'CQ >>> Company {company} is already de-listed. Skipping.')

            elif response.status_code == 200 and response.text != None and response.text.find('"error": "server_error"') != -1:
                logger.error('CQ >>> Non-HTTP server-side exception returned. Aborting!')
                raise Exception()
            
            #this should not happen (ever)
            elif response.status_code == 200 and response.text == None:
                logger.error('CQ >>> Received a null HTTP response text. Aborting!')
                raise Exception()
            
            #response.status_code != 200
            else:
                logger.error(f'CQ >>> HTTP error code received: {response.status_code}. Aborting!')
                raise Exception()
    
    except:
        logger.error('CQ >>> Exception encountered. Aborting!')
        #uncomment for debugging purposes only
        #raise

##main thread start

logger.info('*** Running COMPANY scan script ***')
    
#db file check/backup section
if path.exists(db_file_full_path):
    #create a backup of the existing db - mostly for debugging/recovery
    copy2(db_file_full_path, db_file_full_path + '.bak')
    logger.info('Successfully created DB backup.')
else:
    #subprocess.run(['python', 'gog_create_db.py']) 
    logger.critical('Could find specified DB file!')
    raise Exception()

#conf file check/backup section
if path.exists(conf_file_full_path):
    #create a backup of the existing conf file - mostly for debugging/recovery
    copy2(conf_file_full_path, conf_file_full_path + '.bak')
    logger.info('Successfully created conf file backup.')
else:
    logger.critical('Could find specified conf file!')
    raise Exception()
    
try:
    #reading from config file
    configParser.read(conf_file_full_path)
    #parsing generic parameters
    scan_mode = configParser['GENERAL']['scan_mode']
except:
    logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
    raise Exception()
    
#added support for optional command-line parameter mode switching
try:
    parameter =  argv[1]
    logger.info('Command-line parameter mode override detected.')
    
    if parameter == '-f':
        scan_mode = 'full'
    elif parameter == '-u':
        scan_mode = 'update'
    else:
        logger.error('Invalid command-line parameter option! Mode switch will be ignored!')
except IndexError:
    pass

if scan_mode == 'full':
    logger.info('--- Running in FULL scan mode ---')
    gog_company_query(scan_mode)

elif scan_mode == 'update':
    logger.info('--- Running in UPDATE scan mode ---')
    gog_company_query(scan_mode)
    
logger.info('All done! Exiting...')

##main thread end