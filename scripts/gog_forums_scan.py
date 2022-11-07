#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 3.40
@date: 06/11/2022

Warning: Built for use with python 3.6+
'''

import sqlite3
import signal
import requests
import logging
import argparse
import os
from shutil import copy2
from configparser import ConfigParser
from datetime import datetime
from time import sleep
from lxml import html as lhtml
from logging.handlers import RotatingFileHandler
#uncomment for debugging purposes only
#import traceback

##global parameters init
configParser = ConfigParser()
terminate_signal = False

##conf file block
conf_file_path = os.path.join('..', 'conf', 'gog_forums_scan.conf')

##logging configuration block
log_file_path = os.path.join('..', 'logs', 'gog_forums_scan.log')
logger_file_handler = RotatingFileHandler(log_file_path, maxBytes=25165824, backupCount=1, encoding='utf-8')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler.setFormatter(logging.Formatter(logger_format))
#logging level for other modules
logging.basicConfig(format=logger_format, level=logging.ERROR) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
#logging level for current logger
logger.setLevel(logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger.addHandler(logger_file_handler)

##db configuration block
db_file_path = os.path.join('..', 'output_db', 'gog_gles.db')

##CONSTANTS
INSERT_FORUM_QUERY = 'INSERT INTO gog_forums VALUES (?,?,?,?,?)'

OPTIMIZE_QUERY = 'PRAGMA optimize'

def terminate_script():
    logger.critical('Forcefully stopping script!')
    
    #flush buffers
    os.sync()
    #forcefully terminate script process
    os.kill(os.getpid(), signal.SIGKILL)
        
def gog_forums_query(session, db_connection):
    
    forums_url = 'https://www.gog.com/forum/ajax?a=getArrayList&s=Find%20specific%20forum...&showAll=1'
    
    detected_forum_names = []
    
    try:
        response = session.get(forums_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'FRQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == 200:
            html_tree = lhtml.fromstring(response.text)
            
            parent_divs = html_tree.xpath('//div[contains(@class, "name")]/a[contains(@href, "")]')
            
            for child_div in parent_divs:
                forum_name = child_div.xpath('text()')[0].strip()
                detected_forum_names.append(f'"{forum_name}"')
                #parsed forum links contain a # referece in them, but that's not really worth storing
                forum_link = 'https://www.gog.com' + child_div.xpath('@href')[0].split('#')[0]
                logger.debug(f'FRQ >>> Parsed entry with forum name: {forum_name}, forum link: {forum_link}')
                
                db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_forums WHERE gfr_name = ?', (forum_name, ))
                entry_count = db_cursor.fetchone()[0]
                
                if entry_count == 0:
                    #gfr_int_nr, gfr_int_added, gfr_int_removed, gfr_name, gfr_link
                    db_cursor.execute(INSERT_FORUM_QUERY, (None, datetime.now(), None, forum_name, forum_link))
                    db_connection.commit()
                    logger.info(f'FRQ +++ Added a new DB entry for {forum_name}.')
                    
                elif entry_count == 1:
                    db_cursor.execute('SELECT gfr_int_removed, gfr_link FROM gog_forums WHERE gfr_name = ?', (forum_name, ))
                    existing_delisted, existing_link = db_cursor.fetchone()
                    
                    #clear the delisted status if an id is relisted (should only happen rarely)
                    if existing_delisted is not None:
                        logger.debug(f'FRQ >>> Found a previously removed entry with name {forum_name}. Clearing removed status...')
                        db_cursor.execute('UPDATE gog_forums SET gfr_int_removed = NULL WHERE gfr_name = ?', (forum_name, ))
                        db_connection.commit()
                        logger.info(f'FRQ *** Cleared removed status for {forum_name}.')
                        
                    #this should be very unlikely, yet properly update it if the link gets changed for some reason
                    if existing_link != forum_link:
                        logger.debug(f'FRQ >>> Existing entry for {forum_name} is outdated. Updating...')
                        db_cursor.execute('UPDATE gog_forums SET gfr_link = ? WHERE gfr_name = ?', (forum_link, forum_name))
                        db_connection.commit()
                        logger.info(f'FRQ ~~~ Updated the DB entry for {forum_name}.')
                       
            #general pass to mark undetected but existing entries as removed
            exclusion_list = ', '.join(detected_forum_names)
            
            db_cursor.execute('SELECT COUNT(*) FROM gog_forums WHERE gfr_int_removed IS NULL '
                             f'AND gfr_name NOT IN ({exclusion_list})')
            entry_count = db_cursor.fetchone()[0]
            
            if entry_count == 0:
                logger.debug('FRQ >>> No entries to mark as removed. Skipping.')
            else:
                db_cursor.execute('SELECT gfr_name FROM gog_forums WHERE gfr_int_removed IS NULL ' 
                                 f'AND gfr_name NOT IN ({exclusion_list})')
                forum_name_list = [forum_name[0] for forum_name in db_cursor.fetchall()]
                
                for forum_name in forum_name_list:
                    logger.debug(f'FRQ >>> Forum {forum_name} has been removed...')
                    db_cursor.execute('UPDATE gog_forums SET gfr_int_removed = ? WHERE gfr_name = ?', (datetime.now(), forum_name))
                    db_connection.commit()
                    logger.warning(f'FRQ --- Marked the DB entry for {forum_name} as removed.')
                    
            return True
                    
        else:
            logger.warning(f'FRQ >>> HTTP error code {response.status_code} received.')
            return False
        
    #sometimes the connection may time out
    except requests.Timeout:
        logger.warning(f'FRQ >>> HTTP request timed out after {HTTP_TIMEOUT} seconds.')
        return False
        
    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning('FRQ >>> Connection SSL error encountered.')
        return False
    
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning('FRQ >>> Connection error encountered.')
        return False
    
    except:
        logger.debug('FRQ >>> Forums query has failed.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return False
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description=('GOG forums scan (part of gog_gles) - a script to scrape the GOG website '
                                                  'in order to retrieve existing forums.'))
    
    args = parser.parse_args()
    
    logger.info('*** Running FORUMS scan script ***')
    
    try:
        #reading from config file
        configParser.read(conf_file_path)
        general_section = configParser['GENERAL']
        #parsing generic parameters
        db_backup = general_section.get('db_backup')
        #parsing constants
        HTTP_TIMEOUT = general_section.getint('http_timeout')
        RETRY_COUNT = general_section.getint('retry_count')
        RETRY_SLEEP_INTERVAL = general_section.getint('retry_sleep_interval')
    except:
        logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
        raise SystemExit(1)
    
    #boolean 'true' or scan_mode specific activation
    if db_backup == 'true':
        if os.path.exists(db_file_path):
            #create a backup of the existing db - mostly for debugging/recovery
            copy2(db_file_path, db_file_path + '.bak')
            logger.info('Successfully created db backup.')
        else:
            #subprocess.run(['python', 'gog_create_db.py'])
            logger.critical('Could find specified DB file!')
            raise SystemExit(2)
        
    try:
        logger.info('Starting forums scan...')
        
        with requests.Session() as session, sqlite3.connect(db_file_path) as db_connection:
            retries_complete = False
            retry_counter = 0
                    
            while not retries_complete and not terminate_signal:
                if retry_counter > 0:
                    logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                    sleep(RETRY_SLEEP_INTERVAL)
                    logger.warning(f'Reprocessing forum entries...')
                    
                retries_complete = gog_forums_query(session, db_connection)
                
                if retries_complete:
                    if retry_counter > 0:
                        logger.info(f'Succesfully retried forum entries.')
                
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
    
    logger.info('All done! Exiting...')
