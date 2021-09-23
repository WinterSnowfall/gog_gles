#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 2.50
@date: 23/09/2021

Warning: Built for use with python 3.6+
'''

import json
import html
import threading
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
from html2text import html2text
from datetime import datetime
from time import sleep
from queue import Queue
from collections import OrderedDict
from lxml import html as lhtml
from logging.handlers import RotatingFileHandler
#uncomment for debugging purposes only
#import traceback

##global parameters init
configParser = ConfigParser()
db_lock = threading.Lock()
config_lock = threading.Lock()
terminate_signal = False
reset_id = True

##conf file block
conf_file_full_path = os.path.join('..', 'conf', 'gog_products_scan.conf')

##logging configuration block
log_file_full_path = os.path.join('..', 'logs', 'gog_products_scan.log')
logger_file_handler = RotatingFileHandler(log_file_full_path, maxBytes=8388608, backupCount=1, encoding='utf-8')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler.setFormatter(logging.Formatter(logger_format))
#logging level for other modules
logging.basicConfig(format=logger_format, level=logging.ERROR) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
#logging level for current logger
logger.setLevel(logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger.addHandler(logger_file_handler)

##db configuration block
db_file_full_path = os.path.join('..', 'output_db', 'gog_visor.db')

##CONSTANTS
INSERT_ID_QUERY = 'INSERT INTO gog_products VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

UPDATE_ID_QUERY = ('UPDATE gog_products SET gp_int_updated = ?, '
                   'gp_int_json_payload = ?, '
                   'gp_int_json_diff = ?, '
                   'gp_title = ?, '
                   'gp_slug = ?, '
                   'gp_cs_compat_windows = ?, '
                   'gp_cs_compat_osx = ?, '
                   'gp_cs_compat_linux = ?, '
                   'gp_languages = ?, '
                   'gp_links_forum = ?, '
                   'gp_links_product_card = ?, '
                   'gp_links_support = ?, '
                   'gp_in_development = ?, '
                   'gp_is_installable = ?, '
                   'gp_game_type = ?, '
                   'gp_is_pre_order = ?, '
                   'gp_release_date = ?, '
                   'gp_description_lead = ?, '
                   'gp_description_full = ?, '
                   'gp_description_cool = ?, '
                   'gp_changelog = ? WHERE gp_id = ?')

UPDATE_ID_V2_QUERY = ('UPDATE gog_products SET gp_int_v2_updated = ?, '
                      'gp_int_v2_json_payload = ?, '
                      'gp_int_v2_json_diff = ?, '
                      'gp_v2_developer = ?, '
                      'gp_v2_publisher = ?, '
                      'gp_v2_tags = ?, '
                      'gp_v2_series = ?, '
                      'gp_v2_features = ?, '
                      'gp_v2_is_using_dosbox = ? WHERE gp_id = ?')

INSERT_FILES_QUERY = 'INSERT INTO gog_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

OPTIMIZE_QUERY = 'PRAGMA optimize'

ADALIA_MISSING_URL = 'https://gog.bigpizzapies.com/missingUrls.json'
ADALIA_LEGACY_URL = 'https://gog.bigpizzapies.com/legacyUrls.json'
#number of retries after which an id is considered parmenently delisted (for archive mode)
ARCHIVE_NO_OF_RETRIES = 3
#static regex pattern for endline fixing of extra description/changelog whitespace
ENDLINE_FIX_REGEX = re.compile('([ ]*[\n]){2,}')

#set the gog_lc cookie to avoid errors bought about by GOG dynamically determining the site language
COOKIES = {
    'gog_lc': 'BE_EUR_en-US'
}

def sigterm_handler(signum, frame):
    logger.info('Stopping scan due to SIGTERM...')
    
    raise SystemExit(0)

def gog_product_company_query(product_id, session, db_connection, product_url):
    
    logger.debug(f'CQ >>> Querying url: {product_url}.')
        
    try:
        response = session.get(product_url, cookies=COOKIES, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'CQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == 200:
            logger.debug(f'CQ >>> HTTP response URL: {response.url}.')
            
            #check if the response URL remained identical to with the provided product URL
            if response.url == product_url:
                logger.debug(f'CQ >>> Product company query for id {product_id} has returned a valid response...')
                    
                html_tree = lhtml.fromstring(response.text)
                parent_divs = html_tree.xpath('//div[contains(@class, "table__row")]/div[contains(@class, "details__category") and contains(@class, "table__row-label")]/text()')
                #strip any detected values to fix occasional GOG paging whitespace
                parent_divs = [item.strip() for item in parent_divs]
                logger.debug(f'CQ >>> Found parent elements value: {parent_divs}.')
                
                #check if the 'Company:' tag is present among the parent divs
                if 'Company:' in parent_divs:
                    developer_raw = html_tree.xpath('//div[contains(@class, "details__content") and contains(@class, "table__row-content")]'
                                                    '/a[@class="details__link" and contains(@gog-track-event, "eventLabel: \'Developer:")]/text()')[0]
                    logger.debug(f'CQ >>> Found developer raw value: {developer_raw}.')
                    #unescape any potentially remanent HTML notations such as '&amp;'
                    developer = html.unescape(developer_raw.strip())
                    if developer == '': developer = None
                    
                    publisher_raw = html_tree.xpath('//div[contains(@class, "details__content") and contains(@class, "table__row-content")]'
                                                    '/a[@class="details__link" and contains(@gog-track-event, "eventLabel: \'Publisher:")]/text()')[0]
                    logger.debug(f'CQ >>> Found publisher raw value: {publisher_raw}.')
                    #unescape any potentially remanent HTML notations such as '&amp;'
                    publisher = html.unescape(publisher_raw.strip())
                    if publisher == '': publisher = None
                    
                    db_cursor = db_connection.execute('SELECT gp_v2_developer, gp_v2_publisher, gp_title FROM gog_products WHERE gp_id = ?', (product_id, ))
                    existing_developer, existing_publisher, product_title = db_cursor.fetchone()
                    
                    if existing_developer != developer or existing_publisher != publisher:
                        if developer is not None and publisher is not None:
                            logger.debug(f'CQ >>> Developer/publisher are outdated for {product_id}. Updating...')
                            with db_lock:
                                db_cursor.execute('UPDATE gog_products SET gp_v2_developer = ?, gp_v2_publisher = ? WHERE gp_id = ?', 
                                                  (developer, publisher, product_id))
                                db_connection.commit()
                            logger.info(f'CQ %%% Successfully updated developer/publisher for {product_id}: {product_title}.')
                        else:
                            logger.warning(f'CQ >>> Null developer/publisher values returned for {product_id}. Keeping existing values...')
                    
                else:
                    logger.debug('CQ >>> Unable to find a valid company div section. Perhaps the product is no longer being sold?')
            
            #invalid product URLs will redirect to the GOG games page
            else:
                logger.debug('CQ >>> Product URL has been redirected to the GOG games page. Perhaps the product is no longer being sold?')
        
        else:
            logger.warning(f'CQ >>> HTTP error code {response.status_code} received for {product_id}.')
            raise Exception()
        
    except:
        logger.debug(f'CQ >>> Product company query has failed for {product_id}.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        raise

def gog_product_v2_query(product_id, session, db_connection):
    
    product_url = f'https://api.gog.com/v2/games/{product_id}?locale=en-US'
        
    try:
        response = session.get(product_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'2Q >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == 200:
            logger.debug(f'2Q >>> Product v2 query for id {product_id} has returned a valid response...')
            
            json_v2_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            json_v2_formatted = json.dumps(json_v2_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
            
            db_cursor = db_connection.execute('SELECT gp_int_v2_json_payload FROM gog_products WHERE gp_id = ?', (product_id, ))
            existing_v2_json_formatted = db_cursor.fetchone()[0]
            
            if existing_v2_json_formatted != json_v2_formatted:
                if existing_v2_json_formatted is not None:
                    logger.debug(f'2Q >>> Existing v2 data for {product_id} is outdated. Updating...')
                
                #calculate the diff between the new json and the previous one
                #(applying the diff on the new json will revert to the previous version)
                if existing_v2_json_formatted is not None:
                    diff_v2_formatted = ''.join([line for line in difflib.unified_diff(json_v2_formatted.splitlines(1), 
                                                                                       existing_v2_json_formatted.splitlines(1), n=0)])
                else:
                    diff_v2_formatted = None
                    
                #process product title (for loggers)
                product_title = json_v2_parsed['_embedded']['product']['title']
                #process developer/publisher
                developer = json_v2_parsed['_embedded']['developers'][0]['name'].strip()
                publisher = json_v2_parsed['_embedded']['publisher']['name'].strip()
                #process tags
                tags = ', '.join(sorted([tag['name'] for tag in json_v2_parsed['_embedded']['tags']]))
                if tags == '': tags = None
                #process series - these may be 'null' and return a TypeError in such cases
                try:
                    series = json_v2_parsed['_embedded']['series']['name'].strip()
                except TypeError:
                    series = None
                #process features
                features = ', '.join(sorted([feature['name'] for feature in json_v2_parsed['_embedded']['features']]))
                if features == '': features = None
                #process is_using_dosbox
                is_using_dosbox = json_v2_parsed['isUsingDosBox']
                
                with db_lock:
                    #remove title from the first position in values_formatted and add the id at the end
                    #gp_int_v2_latest_update, gp_int_v2_json_payload, gp_int_v2_previous_json_diff,
                    #gp_v2_developer, gp_v2_publisher, gp_v2_tags, gp_vs_series,
                    #gp_v2_features, gp_v2_is_using_dosbox, gp_id (WHERE clause)
                    db_cursor.execute(UPDATE_ID_V2_QUERY, (datetime.now(), json_v2_formatted, diff_v2_formatted, 
                                                           developer, publisher, tags, series, 
                                                           features, is_using_dosbox, product_id))
                    db_connection.commit()
                    
                if existing_v2_json_formatted is not None:
                    logger.info(f'2Q ~~~ Updated the v2 data for {product_id}: {product_title}.')
                else:
                    logger.info(f'2Q +++ Added v2 data for {product_id}: {product_title}.')
        
        #ids corresponding to movies will return a 404 error, others should not
        elif response.status_code == 404:
            logger.warning(f'2Q >>> Product with id {product_id} returned a HTTP 404 error code. Skipping.')
        
        else:
            logger.warning(f'2Q >>> HTTP error code {response.status_code} received for {product_id}.')
            raise Exception()
        
    except:
        logger.debug(f'2Q >>> Product company query has failed for {product_id}.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        raise
    
def gog_product_extended_query(product_id, scan_mode, session, db_connection):
    
    product_url = f'https://api.gog.com/products/{product_id}?expand=downloads,expanded_dlcs,description,screenshots,videos,related_products,changelog'
    
    try:
        response = session.get(product_url, timeout=HTTP_TIMEOUT)
            
        logger.debug(f'PQ >>> HTTP response code: {response.status_code}.')
            
        if response.status_code == 200:
            if scan_mode == 'full':
                logger.info(f'PQ >>> Product query for id {product_id} has returned a valid response...')
            
            db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_products WHERE gp_id = ?', (product_id, ))
            entry_count = db_cursor.fetchone()[0]
            
            #initialize is movie with False
            is_movie = False
            
            #no need to do any processing if an entry is found in 'full' scan mode, 
            #since that entry will be skipped anyway
            if not (entry_count == 1 and (scan_mode == 'full' or scan_mode == 'builds')):
                
                json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
                json_formatted = json.dumps(json_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                
                #process unmodified fields
                #product_id = json_parsed['id']
                product_title = json_parsed['title'].strip()
                product_slug = json_parsed['slug']
                #process content_system_compatibility
                cs_compat_windows = json_parsed['content_system_compatibility']['windows']
                cs_compat_osx = json_parsed['content_system_compatibility']['osx']
                cs_compat_linux = json_parsed['content_system_compatibility']['linux']
                #process languages
                if len(json_parsed['languages']) > 0:
                    languages = ', '.join([f'{language_key}: {json_parsed["languages"][language_key]}' 
                                           for language_key in json_parsed['languages'].keys()])
                else:
                    languages = None
                #process links
                links_forum = json_parsed['links']['forum']
                links_product_card = json_parsed['links']['product_card']
                links_support = json_parsed['links']['support']
                #process unmodified fields
                in_development = json_parsed['in_development']['active']
                is_installable = json_parsed['is_installable']
                game_type = json_parsed['game_type']
                is_pre_order = json_parsed['is_pre_order']
                release_date = json_parsed['release_date']
                #need to correct some GOG formatting wierdness by using regular expressions
                #process description
                try:
                    description_lead = ENDLINE_FIX_REGEX.sub('\n\n', html2text(json_parsed['description']['lead'], bodywidth=0).strip())
                except AttributeError:
                    description_lead = None
                if description_lead == '': description_lead = None
                try:
                    description_full = ENDLINE_FIX_REGEX.sub('\n\n', html2text(json_parsed['description']['full'], bodywidth=0).strip())
                except AttributeError:
                    description_full = None
                if description_full == '': description_full = None
                #appears to be treated like a per row input for a HTML list shown on the product webpage
                try:
                    description_cool = ENDLINE_FIX_REGEX.sub('\n\n', html2text(json_parsed['description']['whats_cool_about_it']
                                                                               .replace('\n', '<br><br>'), bodywidth=0).strip())
                except AttributeError:
                    description_cool = None
                if description_cool == '': description_cool = None
                #process changelog
                try:
                    changelog = ENDLINE_FIX_REGEX.sub('\n\n', html2text(json_parsed['changelog'], bodywidth=0).strip())
                except AttributeError:
                    changelog = None
                if changelog == '': changelog = None
                
                #if the API returned product title starts with 'product_title_', keep the existing product title
                if product_title is not None and product_title.startswith('product_title_'):
                    logger.warning('PQ >>> Product title update skipped since an invalid value was returned.')
                    db_cursor.execute('SELECT gp_title FROM gog_products WHERE gp_id = ?', (product_id, ))
                    product_title = db_cursor.fetchone()[0]
                
                #detect if the entry is a movie based on the links_forum value and then on the content of the 
                #lead description field, since the APIs ofer no discrimination (it's not pretty, but it works)
                if ((links_forum == 'https://www.gog.com/forum/movies' or 
                   (description_lead is not None and description_lead.startswith('IMDB rating:')) or 
                   product_id in STATIC_MOVIES_ID_LIST) 
                   and not product_id in STATIC_NON_MOVIES_ID_LIST):
                    is_movie = True
                                
            if entry_count == 0:
                with db_lock:
                    #gp_int_nr, gp_int_added, gp_int_delisted, gp_int_updated, gp_int_json_payload, 
                    #gp_int_json_diff, gp_int_v2_updated, gp_int_v2_json_payload, 
                    #gp_int_v2_json_diff, gp_int_is_movie, gp_v2_developer, gp_v2_publisher,
                    #gp_v2_tags, gp_v2_series, gp_v2_features, gp_v2_is_using_dosbox,
                    #gp_id, gp_title, gp_slug, gp_cs_compat_windows, 
                    #gp_cs_compat_osx, gp_cs_compat_linux, gp_languages, gp_links_forum, 
                    #gp_links_product_card, gp_links_support, gp_in_development, gp_is_installable, 
                    #gp_game_type, gp_is_pre_order, gp_release_date, gp_description_lead, 
                    #gp_description_full, gp_description_cool, gp_changelog
                    db_cursor.execute(INSERT_ID_QUERY, (None, datetime.now(), None, None, json_formatted,
                                                        None, None, None, None, is_movie, None, None, 
                                                        None, None, None, None,
                                                        product_id, product_title, product_slug, cs_compat_windows, 
                                                        cs_compat_osx, cs_compat_linux, languages, links_forum, 
                                                        links_product_card, links_support, in_development, is_installable, 
                                                        game_type, is_pre_order, release_date, description_lead, 
                                                        description_full, description_cool, changelog))
                    db_connection.commit()
                logger.info(f'PQ +++ Added a new DB entry for {product_id}: {product_title}.')
                
                #movies do not have a valid v2 product API entry
                if not is_movie:
                    #call the v2 api query to save the v2 json payload and populate developer/publisher values
                    gog_product_v2_query(product_id, session, db_connection)
                    #fall back to website scraping of developer/publisher values for movies
                elif links_forum is not None:
                    gog_product_company_query(product_id, session, db_connection, links_forum.replace('/game/', '/movie/'))
            
            elif entry_count == 1:
                #do not update existing entries in a full scan, since update/delta scans will take care of that
                if scan_mode == 'full':
                    logger.info(f'PQ >>> Found an existing db entry with id {product_id}. Skipping.')
                #manual scans will be treated as update scans
                else:
                    db_cursor.execute('SELECT gp_int_delisted, gp_int_json_payload FROM gog_products WHERE gp_id = ?', (product_id, ))
                    existing_delisted, existing_json_formatted = db_cursor.fetchone()
                    
                    #clear the delisted status if an id is relisted (should only happen rarely)
                    if existing_delisted is not None:
                        logger.debug(f'PQ >>> Found a previously delisted entry with id {product_id}. Removing delisted status...')
                        with db_lock:
                            db_cursor.execute('UPDATE gog_products SET gp_int_delisted = NULL WHERE gp_id = ?', (product_id, ))
                            db_connection.commit()
                        logger.info(f'PQ *** Removed delisted status for {product_id}: {product_title}')
                    
                    if existing_json_formatted != json_formatted:
                        logger.debug(f'PQ >>> Existing entry for {product_id} is outdated. Updating...')
                        
                        #calculate the diff between the new json and the previous one
                        #(applying the diff on the new json will revert to the previous version)
                        if existing_json_formatted is not None:
                            diff_formatted = ''.join([line for line in difflib.unified_diff(json_formatted.splitlines(1), 
                                                                                            existing_json_formatted.splitlines(1), n=0)])
                        else:
                            diff_formatted = None
                        
                        with db_lock:
                            #gp_int_updated, gp_int_json_payload, gp_int_json_diff, gp_title, 
                            #gp_slug, gp_cs_compat_windows, gp_cs_compat_osx, gp_cs_compat_linux, 
                            #gp_languages, gp_links_forum, gp_links_product_card, gp_links_support, 
                            #gp_in_development, gp_is_installable, #gp_game_type, gp_is_pre_order, gp_release_date, 
                            #gp_description_lead, gp_description_full, gp_description_cool, gp_changelog, gp_id (WHERE clause)
                            db_cursor.execute(UPDATE_ID_QUERY, (datetime.now(), json_formatted, diff_formatted, product_title, 
                                                                product_slug, cs_compat_windows, cs_compat_osx, cs_compat_linux, 
                                                                languages, links_forum, links_product_card, links_support, 
                                                                in_development, is_installable, game_type, is_pre_order, release_date, 
                                                                description_lead, description_full, description_cool, changelog, product_id))
                            db_connection.commit()
                        logger.info(f'PQ ~~~ Updated the DB entry for {product_id}: {product_title}.')
            
                    #movies do not have a valid v2 product API entry
                    if not is_movie:
                        #call the v2 api query to save the v2 json payload and update developer/publisher values
                        gog_product_v2_query(product_id, session, db_connection)
                    #fall back to website scraping of developer/publisher values for movies
                    elif links_forum is not None:
                        gog_product_company_query(product_id, session, db_connection, links_forum.replace('/game/', '/movie/'))
                            
        #existing ids return a 404 HTTP error code on removal
        elif scan_mode == 'update' and response.status_code == 404:
            #check to see the existing value for gp_int_no_longer_listed
            db_cursor = db_connection.execute('SELECT gp_int_delisted, gp_title FROM gog_products WHERE gp_id = ?', (product_id, ))
            existing_delisted, product_title = db_cursor.fetchone()
            
            #only alter the entry if not already marked as no longer listed
            if existing_delisted is None:
                logger.debug(f'PQ >>> Product with id {product_id} has been delisted...')
                with db_lock:
                    db_cursor.execute('UPDATE gog_products SET gp_int_delisted = ? WHERE gp_id = ?', (datetime.now(), product_id))
                    db_connection.commit()
                logger.warning(f'PQ --- Delisted the DB entry for: {product_id}: {product_title}.')
            else:
                logger.debug(f'PQ >>> Product with id {product_id} is already marked as delisted.')
                    
        #unmapped ids will also return a 404 HTTP error code
        elif response.status_code == 404:
            logger.debug(f'PQ >>> Product with id {product_id} returned a HTTP 404 error code. Skipping.')
        
        else:
            logger.warning(f'PQ >>> HTTP error code {response.status_code} received for {product_id}.')
            raise Exception()
        
        return True

    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'PQ >>> Connection SSL error encountered for {product_id}.')
        return False
            
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'PQ >>> Connection error encountered for {product_id}.')
        return False
            
    except:
        logger.debug(f'PQ >>> Product extended query has failed for {product_id}.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return False
    
def gog_product_games_ajax_query(url, scan_mode, session, db_connection):
    
    logger.info(f'GQ >>> Querying url: {url}.')
    
    #return a value of 0, should something go terribly wrong
    totalPages = 0
    
    try:
        response = session.get(url, cookies=COOKIES, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'GQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == 200:
            gogData_json = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            #return the total number of pages, as listed in the response
            totalPages = gogData_json['totalPages']
            logger.debug(f'GQ >>> Total pages: {totalPages}.')
                        
            #use a set to avoid processing potentially duplicate ids
            id_set = set()
            
            for product_element in gogData_json['products']:
                id_value = product_element['id']
                logger.debug(f'GQ >>> Found the following id: {id_value}.')
                id_set.add(id_value)
            
            #sort the set into an ordered list
            id_list = sorted(id_set)
                
            for product_id in id_list:
                logger.debug(f'GQ >>> Running scan for id {product_id}...')
                retries_complete = False
                retry_counter = 0
                
                while not retries_complete:
                    if retry_counter > 0:
                        logger.warning(f'GQ >>> Reprocessing id {product_id}...')
                        #allow a short respite before re-processing
                        sleep(2)
                    
                    retries_complete = gog_product_extended_query(product_id, scan_mode, session, db_connection)
                    
                    if not retries_complete:
                        retry_counter += 1
             
        else:
            logger.warning(f'GQ >>> HTTP error code {response.status_code} received.')
            raise Exception()
        
        return totalPages
    
    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'GQ >>> Connection SSL error encountered for {product_id}.')
        return 0
    
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'GQ >>> Connection error encountered for {product_id}.')
        return 0
    
    except:
        logger.critical('GQ >>> Processing has failed!')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return 0
    
def gog_products_third_party_query(third_party_url, scan_mode, session, db_connection):
    
    logger.info(f'TQ >>> Querying url: {third_party_url}.')
    
    try:
        response = session.get(third_party_url, timeout=HTTP_TIMEOUT)
            
        logger.debug(f'TQ >>> HTTP response code: {response.status_code}.')
            
        if response.status_code == 200:
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            db_cursor = db_connection.cursor()
            
            for extra_id_raw in json_parsed:
                extra_id = extra_id_raw.strip()
                if extra_id == '': extra_id = None
                
                logger.debug(f'TQ >>> Picked up the following product id: {extra_id}.')
                
                #at least one of the ids is null for some reason, so do check
                if extra_id is not None:
                    db_cursor.execute('SELECT COUNT(*) FROM gog_products WHERE gp_id = ?', (extra_id, ))
                    entry_count = db_cursor.fetchone()[0]
                    
                    #only run a product scan if a new id was detected
                    if entry_count == 0:
                        logger.debug(f'TQ >>> Unknown id detected! Running scan for {extra_id}.')
                        retries_complete = False
                        retry_counter = 0
                            
                        while not retries_complete:
                            if retry_counter > 0:
                                logger.warning(f'TQ >>> Reprocessing id {extra_id}...')
                                #allow a short respite before re-processing
                                sleep(2)
                            
                            retries_complete = gog_product_extended_query(extra_id, scan_mode, session, db_connection)
                            
                            if not retries_complete:
                                retry_counter += 1
                    
                    else:
                        logger.debug('TQ >>> The id is already present in the gog_products table. Skipping!')
                 
        else:
            logger.warning(f'TQ >>> HTTP error code {response.status_code} received.')
            raise Exception()
    
    except:
        logger.critical('TQ >>> Processing has failed!')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        raise
        
def gog_files_extract_parser(db_connection, product_id):
    
    db_cursor = db_connection.execute('SELECT gp_int_json_payload FROM gog_products WHERE gp_id = ?', (product_id, ))
    json_payload = db_cursor.fetchone()[0]
    
    json_parsed = json.loads(json_payload, object_pairs_hook=OrderedDict)
    
    #extract installer entries
    json_parsed_installers = json_parsed['downloads']['installers']
    #extract patch entries
    json_parsed_patches = json_parsed['downloads']['patches']
    #extract language_packs entries
    json_parsed_language_packs = json_parsed['downloads']['language_packs']
    #extract bonus_content entries
    json_parsed_bonus_content = json_parsed['downloads']['bonus_content']
    
    #process installer entries
    db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? '
                      'AND gf_int_download_type = "installer" AND gf_int_removed IS NULL', (product_id,))
    listed_installer_pks = [pk_result[0] for pk_result in db_cursor.fetchall()]
        
    for installer_entry in json_parsed_installers:
        installer_id = installer_entry['id']
        installer_product_name = installer_entry['name'].strip()
        installer_os = installer_entry['os']
        installer_language = installer_entry['language']
        try:
            installer_version = installer_entry['version'].strip()
        except AttributeError:
            installer_version = None
        installer_total_size = installer_entry['total_size']
        
        for installer_file in installer_entry['files']:
            installer_file_id = installer_file['id']
            installer_file_size = installer_file['size']
            
            if installer_version is not None:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = "installer" AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version = ? AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, installer_id, installer_os, installer_language, installer_version, installer_file_id, installer_file_size))
            else:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = "installer" AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version IS NULL AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, installer_id, installer_os, installer_language, installer_file_id, installer_file_size))
              
            entry_pk = db_cursor.fetchone()
            
            if entry_pk is None:
                #gf_int_nr, gf_int_added, gf_int_removed, gf_int_id, gf_int_download_type,
                #gf_id, gf_name, gf_os, gf_language, gf_version,
                #gf_type, gf_count, gf_total_size, gf_file_id, gf_file_size
                db_cursor.execute(INSERT_FILES_QUERY, (None, datetime.now(), None, product_id, 'installer', 
                                                       installer_id, installer_product_name, installer_os, installer_language, installer_version,
                                                       None, None, installer_total_size, installer_file_id, installer_file_size))
                #no need to print the os here, as it's included in the installer_id
                logger.info(f'FQ +++ Added DB entry for {product_id}: {installer_product_name}, {installer_id}, {installer_version}.')
            
            else:
                logger.debug(f'FQ >>> Found an existing entry for {product_id}: {installer_product_name}, {installer_id}, {installer_version}.')
                listed_installer_pks.remove(entry_pk[0])
                
    if len(listed_installer_pks) > 0:
        for removed_pk in listed_installer_pks:
            db_cursor.execute('UPDATE gog_files SET gf_int_removed = ? WHERE gf_int_nr = ? AND gf_int_removed IS NULL', (datetime.now(), removed_pk))
        
        logger.info(f'FQ --- Marked some installer entries as removed for {product_id}')
    
    #process patch entries
    db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? '
                      'AND gf_int_download_type = "patch" AND gf_int_removed IS NULL', (product_id,))
    listed_patch_pks = [pk_result[0] for pk_result in db_cursor.fetchall()]
    
    for patch_entry in json_parsed_patches:
        patch_id = patch_entry['id']
        patch_product_name = patch_entry['name'].strip()
        patch_os = patch_entry['os']
        patch_language = patch_entry['language']
        try:
            patch_version = patch_entry['version'].strip()
        except AttributeError:
            patch_version = None
        #replace blank patch version with None (blanks happens with patches, but not with installers)
        if patch_version == '': patch_version = None
        patch_total_size = patch_entry['total_size']
        
        for patch_file in patch_entry['files']:
            patch_file_id = patch_file['id']
            patch_file_size = patch_file['size']
                
            if patch_version is not None:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = "patch" AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version = ? AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, patch_id, patch_os, patch_language, patch_version, patch_file_id, patch_file_size))
            else:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = "patch" AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version IS NULL AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, patch_id, patch_os, patch_language, patch_file_id, patch_file_size))
                
            entry_pk = db_cursor.fetchone()
            
            if entry_pk is None:
                #gf_int_nr, gf_int_added, gf_int_removed, gf_int_id, gf_int_download_type,
                #gf_id, gf_name, gf_os, gf_language, gf_version,
                #gf_type, gf_count, gf_total_size, gf_file_id, gf_file_size
                db_cursor.execute(INSERT_FILES_QUERY, (None, datetime.now(), None, product_id, 'patch', 
                                                       patch_id, patch_product_name, patch_os, patch_language, patch_version,
                                                       None, None, patch_total_size, patch_file_id, patch_file_size))
                #no need to print the os here, as it's included in the patch_id
                logger.info(f'FQ +++ Added DB entry for {product_id}: {patch_product_name}, {patch_id}, {patch_version}.')
            
            else:
                logger.debug(f'FQ >>> Found an existing entry for {product_id}: {patch_product_name}, {patch_id}, {patch_version}.')
                listed_patch_pks.remove(entry_pk[0])
                
    if len(listed_patch_pks) > 0:
        for removed_pk in listed_patch_pks:
            db_cursor.execute('UPDATE gog_files SET gf_int_removed = ? WHERE gf_int_nr = ? AND gf_int_removed IS NULL', (datetime.now(), removed_pk))
        
        logger.info(f'FQ --- Marked some patch entries as removed for {product_id}')
    
    #process language_packs entries
    db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? '
                      'AND gf_int_download_type = "language_packs" AND gf_int_removed IS NULL', (product_id,))
    listed_language_packs_pks = [pk_result[0] for pk_result in db_cursor.fetchall()]
    
    for language_pack_entry in json_parsed_language_packs:
        language_pack_id = language_pack_entry['id']
        language_pack_product_name = language_pack_entry['name'].strip()
        language_pack_os = language_pack_entry['os']
        language_pack_language = language_pack_entry['language']
        try:
            language_pack_version = language_pack_entry['version'].strip()
        except AttributeError:
            language_pack_version = None
        language_pack_total_size = language_pack_entry['total_size']
        
        for language_pack_file in language_pack_entry['files']:
            language_pack_file_id = language_pack_file['id']
            language_pack_file_size = language_pack_file['size']
                
            if language_pack_version is not None:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = "language_packs" AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version = ? AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, language_pack_id, language_pack_os, language_pack_language, language_pack_version, 
                                   language_pack_file_id, language_pack_file_size))
            else:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = "language_packs" AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version IS NULL AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, language_pack_id, language_pack_os, language_pack_language, 
                                   language_pack_file_id, language_pack_file_size))
                
            entry_pk = db_cursor.fetchone()
            
            if entry_pk is None:
                #gf_int_nr, gf_int_added, gf_int_removed, gf_int_id, gf_int_download_type, gf_id,
                #gf_name, gf_os, gf_language, gf_version,
                #gf_type, gf_count, gf_total_size, gf_file_id, gf_file_size
                db_cursor.execute(INSERT_FILES_QUERY, (None, datetime.now(), None, product_id, 'language_packs', language_pack_id, 
                                                       language_pack_product_name, language_pack_os, language_pack_language, language_pack_version,
                                                       None, None, language_pack_total_size, language_pack_file_id, language_pack_file_size))
                #no need to print the os here, as it's included in the patch_id
                logger.info(f'FQ +++ Added DB entry for {product_id}: {language_pack_product_name}, {language_pack_id}, {language_pack_version}.')
                
            else:
                logger.debug(f'FQ >>> Found an existing entry for {product_id}: {language_pack_product_name}, {language_pack_id}, {language_pack_version}.')
                listed_language_packs_pks.remove(entry_pk[0])
                
    if len(listed_language_packs_pks) > 0:
        for removed_pk in listed_language_packs_pks:
            db_cursor.execute('UPDATE gog_files SET gf_int_removed = ? WHERE gf_int_nr = ? AND gf_int_removed IS NULL', (datetime.now(), removed_pk))
        
        logger.info(f'FQ --- Marked some language_pack entries as removed for {product_id}')
                
    #process bonus_content entries
    db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? '
                      'AND gf_int_download_type = "bonus_content" AND gf_int_removed IS NULL', (product_id,))
    listed_bonus_content_pks = [pk_result[0] for pk_result in db_cursor.fetchall()]
    
    for bonus_content_entry in json_parsed_bonus_content:
        bonus_content_id = bonus_content_entry['id']
        bonus_content_product_name = bonus_content_entry['name'].strip()
        #bonus content type 'guides & reference ' has a trailing space
        bonus_content_type = bonus_content_entry['type'].strip()
        bonus_content_count = bonus_content_entry['count']
        bonus_content_total_size = bonus_content_entry['total_size']
        
        for bonus_content_file in bonus_content_entry['files']:
            bonus_content_file_id = bonus_content_file['id']
            bonus_content_file_size = bonus_content_file['size']
            
            db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = "bonus_content" AND gf_id = ? '
                              'AND gf_type = ? AND gf_count = ? AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                              (product_id, bonus_content_id, bonus_content_type, bonus_content_count, bonus_content_file_id, bonus_content_file_size))
            
            entry_pk = db_cursor.fetchone()
            
            if entry_pk is None:
                #gf_int_nr, gf_int_added, gf_int_removed, gf_int_id, gf_int_download_type,
                #gf_id, gf_name, gf_os, gf_language, gf_version,
                #gf_type, gf_count, gf_total_size, gf_file_id, gf_file_size
                db_cursor.execute(INSERT_FILES_QUERY, (None, datetime.now(), None, product_id, 'bonus_content', 
                                                       bonus_content_id, bonus_content_product_name, None, None, None,
                                                       bonus_content_type, bonus_content_count, bonus_content_total_size, 
                                                       bonus_content_file_id, bonus_content_file_size))
                #print the entry type, since bonus_content entries are not versioned
                logger.info(f'FQ +++ Added DB entry for {product_id}: {bonus_content_product_name}, {bonus_content_id}, {bonus_content_type}.')
                
            else:
                logger.debug(f'FQ >>> Found an existing entry for {product_id}: {bonus_content_product_name}, {bonus_content_id}, {bonus_content_type}.')
                listed_bonus_content_pks.remove(entry_pk[0])
                
    if len(listed_bonus_content_pks) > 0:
        for removed_pk in listed_bonus_content_pks:
            db_cursor.execute('UPDATE gog_files SET gf_int_removed = ? WHERE gf_int_nr = ? AND gf_int_removed IS NULL', (datetime.now(), removed_pk))
        
        logger.info(f'FQ --- Marked some bonus_content entries as removed for {product_id}')
                
    #batch commit
    db_connection.commit()

def gog_products_bulk_query(product_id, scan_mode, session, db_connection):
    
    #generate a string of comma separated ids in the current batch
    product_ids_string = ','.join([str(product_id_value) for product_id_value in range(product_id, product_id + IDS_IN_BATCH)])
    logger.debug(f'BQ >>> Processing the following product_id string batch: {product_ids_string}.')
    
    bulk_products_url = f'https://api.gog.com/products?ids={product_ids_string}'
    
    try:
        response = session.get(bulk_products_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'BQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == 200 and response.text != '[]':
            logger.info(f'BQ >>> Found something in the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range...')
            
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            for line in json_parsed:
                current_product_id = line['id']
                retries_complete = False
                retry_counter = 0
                    
                while not retries_complete:
                    if retry_counter > 0:
                        logger.warning(f'BQ >>> Reprocessing id {current_product_id}...')
                        #allow a short respite before re-processing
                        sleep(2)
                    
                    retries_complete = gog_product_extended_query(current_product_id, scan_mode, session, db_connection)
                    
                    if retries_complete:
                        if retry_counter > 1:
                            logger.info(f'BQ >>> Succesfully retried for {current_product_id}.')
                    else:
                        retry_counter += 1
        
        #this should not be handled as an exception, as it's the default behavior when nothing is detected
        elif response.status_code == 200 and response.text == '[]':
            logger.debug('BQ >>> A blank list entry ([]) received.')
        
        else:
            logger.warning(f'BQ >>> HTTP error code {response.status_code} received for the {product_id} '
                           f'<-> {product_id + IDS_IN_BATCH - 1} range.')
            raise Exception()
                
        return True
    
    #sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'BQ >>> Connection SSL error encountered for the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range.')
        return False
    
    #sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'BQ >>> Connection error encountered for the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range.')
        return False
    
    except:
        logger.debug(f'BQ >>> Products bulk query has failed for the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range.')
        #uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        
        return False
            
def worker_thread(thread_number, scan_mode):
    global terminate_signal
    
    threadConfigParser = ConfigParser()
        
    with requests.Session() as threadSession:
        with sqlite3.connect(db_file_full_path) as thread_db_connection:
            while not terminate_signal:
                product_id = queue.get()
                
                retry_counter = 0
                retries_complete = False
                
                while not retries_complete and not terminate_signal:
                    if retry_counter > 0:
                        #terminate the scan if the RETRY_COUNT limit is exceeded
                        if retry_counter > RETRY_COUNT:
                            logger.critical(f'T#{thread_number} >>> Request most likely blocked/invalidated by GOG. Terminating process!')
                            terminate_signal = True
                            break
                    
                        logger.debug(f'T#{thread_number} >>> Retry count: {retry_counter}.')
                        #main iternation incremental sleep
                        sleep((retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL)
                    
                    retries_complete = gog_products_bulk_query(product_id, scan_mode, threadSession, thread_db_connection)
                    
                    if retries_complete:
                        if retry_counter > 0:
                            logger.info(f'T#{thread_number} >>> Succesfully retried for the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range.')
                    else:
                        retry_counter += 1
                    
                if not terminate_signal and product_id != 0 and product_id % ID_SAVE_INTERVAL == 0:
                    with config_lock:
                        threadConfigParser.read(conf_file_full_path)
                        threadConfigParser['FULL_SCAN']['start_id'] = str(product_id)
                        
                        with open(conf_file_full_path, 'w') as file:
                            threadConfigParser.write(file)
                            
                        logger.info(f'T#{thread_number} >>> Processed up to id: {product_id}...')
                
                queue.task_done()

            logger.debug('Running PRAGMA optimize...')
            thread_db_connection.execute(OPTIMIZE_QUERY)
            
##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description=('GOG products scan (part of gog_visor) - a script to call publicly available GOG APIs '
                                              'in order to retrieve product information and updates.'))

group = parser.add_mutually_exclusive_group()
group.add_argument('-n', '--new', help='Query new products', action='store_true')
group.add_argument('-u', '--update', help='Run an update scan for existing products', action='store_true')
group.add_argument('-f', '--full', help='Perform a full products scan using the Galaxy products endpoint', action='store_true')
group.add_argument('-m', '--manual', help='Perform a manual products scan', action='store_true')
group.add_argument('-t', '--third_party', help='Perform a third-party (Adalia Fundamentals) products scan', action='store_true')
group.add_argument('-e', '--extract', help='Extract file data from existing products', action='store_true')
group.add_argument('-d', '--delisted', help='Perform a scan on all the delisted products', action='store_true')

args = parser.parse_args()

logger.info('*** Running PRODUCTS scan script ***')
    
try:
    #reading from config file
    configParser.read(conf_file_full_path)
    general_section = configParser['GENERAL']
    #parsing generic parameters
    conf_backup = general_section.getboolean('conf_backup')
    db_backup = general_section.getboolean('db_backup')
    scan_mode = general_section.get('scan_mode')
    #parsing constants
    HTTP_TIMEOUT = general_section.getint('http_timeout')
    RETRY_COUNT = general_section.getint('retry_count')
    RETRY_SLEEP_INTERVAL = general_section.getint('retry_sleep_interval')
    RETRY_AMPLIFICATION_FACTOR = general_section.getint('retry_amplification_factor')
    #used as a workaround in movies detection logic - ids will always be treated as movies
    STATIC_MOVIES_ID_LIST= [int(product_id.strip()) for product_id in 
                            general_section.get('static_movies_id_list').split(',')]
    #used as a workaround in movies detection logic - ids will always be treated as non-movies
    STATIC_NON_MOVIES_ID_LIST = [int(product_id.strip()) for product_id in 
                                 general_section.get('static_non_movies_id_list').split(',')]
except:
    logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
    raise SystemExit(1)

#detect any parameter overrides and set the scan_mode accordingly
if len(argv) > 1:
    logger.info('Command-line parameter mode override detected.')
    
    if args.new:
        scan_mode = 'new'
    elif args.update:
        scan_mode = 'update'
    elif args.full:
        scan_mode = 'full'
    elif args.manual:
        scan_mode = 'manual'
    elif args.third_party:
        scan_mode = 'third_party'
    elif args.extract:
        scan_mode = 'extract'
    elif args.delisted:
        scan_mode = 'delisted'

if conf_backup:
    #conf file check/backup section
    if os.path.exists(conf_file_full_path):
        #create a backup of the existing conf file - mostly for debugging/recovery
        copy2(conf_file_full_path, conf_file_full_path + '.bak')
        logger.info('Successfully created conf file backup.')
    else:
        logger.critical('Could find specified conf file!')
        raise SystemExit(2)

if db_backup:
    #db file check/backup section
    if os.path.exists(db_file_full_path):
        #create a backup of the existing db - mostly for debugging/recovery
        copy2(db_file_full_path, db_file_full_path + '.bak')
        logger.info('Successfully created db backup.')
    else:
        #subprocess.run(['python', 'gog_create_db.py'])
        logger.critical('Could find specified DB file!')
        raise SystemExit(3)
    
if scan_mode == 'full':
    logger.info('--- Running in FULL scan mode ---')
    
    #catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    
    #theads sync (on exit) timeout interval (seconds)
    THREAD_SYNC_TIMEOUT = 30
    
    full_scan_section = configParser['FULL_SCAN']
    ID_SAVE_INTERVAL = full_scan_section.getint('id_save_interval')
    #50 is the max id batch size allowed by the bulk products API 
    IDS_IN_BATCH = full_scan_section.getint('ids_in_batch')
    #number of active connection threads
    CONNECTION_THREADS = full_scan_section.getint('connection_threads')
    #stop_id = 2147483647, in order to scan the full range,
    #stopping at the upper limit of a 32 bit signed integer type
    stop_id = full_scan_section.getint('stop_id')
    #product_id will restart from scan_id
    product_id = full_scan_section.getint('start_id')
    #reduce starting point by a batch to account for any thread overlap
    if product_id > ID_SAVE_INTERVAL: product_id -= ID_SAVE_INTERVAL
    
    logger.info(f'Restarting scan from id: {product_id}.')
    
    queue = Queue(CONNECTION_THREADS * 2)
    
    try:
        for thread_no in range(CONNECTION_THREADS):
            #apply spacing to single digit thread_no for nicer logging in case of 10+ threads
            THREAD_LOGGING_FILLER = '0' if CONNECTION_THREADS > 9 and thread_no < 9 else ''
            thread_no_nice = THREAD_LOGGING_FILLER + str(thread_no + 1)
            
            logger.info(f'Starting thread T#{thread_no_nice}...')
            #setting daemon threads and a max limit to the thread sync on exit interval will prevent lockups
            thread = threading.Thread(target=worker_thread, args=(thread_no_nice, scan_mode), daemon=True)
            thread.start()
    
        while not terminate_signal and product_id <= stop_id:
            #pass only the start product_id for the current batch
            queue.put(product_id)
            #skip an IDS_IN_BATCH interval
            product_id += IDS_IN_BATCH
                
        #simulate a regular keyboard stop when stop_id is reached
        if product_id > stop_id:
            logger.info(f'Stop id of {stop_id} reached. Halting processing...')
            
            #write the stop_id as the start_id in the config file
            configParser.read(conf_file_full_path)
            configParser['FULL_SCAN']['start_id'] = str(product_id)
            
            with open(conf_file_full_path, 'w') as file:
                configParser.write(file)
            
            raise KeyboardInterrupt
                
    except KeyboardInterrupt:
        terminate_signal = True
        terminate_sync_counter = 0
        
        logger.info('Waiting for all threads to complete...')
        #sleep until all threads except the main thread finish processing
        while threading.activeCount() > 1 and terminate_sync_counter <= THREAD_SYNC_TIMEOUT:
            sleep(1)
            terminate_sync_counter += 1
        
        if terminate_sync_counter > THREAD_SYNC_TIMEOUT:
            logger.warning('Thread sync on exit interval exceeded! Any stuck threads will now be terminated.')
        
elif scan_mode == 'update':
    logger.info('--- Running in UPDATE scan mode ---')
    
    update_scan_section = configParser['UPDATE_SCAN']
    last_id = update_scan_section.getint('last_id')
    ID_SAVE_FREQUENCY = update_scan_section.getint('id_save_frequency')
    
    if last_id > 0:
        logger.info(f'Restarting update scan from id: {last_id}.')
    
    try:
        logger.info('Starting update scan on all existing DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            #skip products which are no longer listed
            db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id > ? '
                                              'AND gp_int_delisted IS NULL ORDER BY 1', (last_id, ))
            id_list = db_cursor.fetchall()
            logger.debug('Retrieved all existing product ids from the DB...')
            
            #track the number of processed ids
            last_id_counter = 0
                
            with requests.Session() as session:
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete:
                        if retry_counter > 0:
                            logger.warning(f'Reprocessing id {current_product_id}...')
                            #allow a short respite before re-processing
                            sleep(2)
                            
                        retries_complete = gog_product_extended_query(current_product_id, scan_mode, session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                            
                            last_id_counter += 1
                            
                        else:
                            retry_counter += 1
                            
                    if last_id_counter != 0 and last_id_counter % ID_SAVE_FREQUENCY == 0:
                        configParser.read(conf_file_full_path)
                        configParser['UPDATE_SCAN']['last_id'] = str(current_product_id)
                        
                        with open(conf_file_full_path, 'w') as file:
                            configParser.write(file)
                            
                        logger.info(f'Saved scan up to last_id of {current_product_id}.')
            
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        reset_id = False
    
elif scan_mode == 'new':
    logger.info('--- Running in NEW scan mode ---')
    
    try:
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                page_no = 1
                #start off as 1, then use whatever is returned by the ajax call
                games_new_url_page_count = 1
                #new games may number above 50 entries and can be split across 2+ pages in the ajax call
                while games_new_url_page_count != 0 and page_no <= games_new_url_page_count:
                    games_new_url = f'https://www.gog.com/games/ajax/filtered?availability=new&mediaType=game&page={page_no}&sort=date'
                    #parse new ids from the games page ajax call
                    games_new_url_page_count = gog_product_games_ajax_query(games_new_url, scan_mode, session, db_connection)
                    page_no += 1
                
                page_no = 1
                #start off as 1, then use whatever is returned by the ajax call
                games_upcoming_url_page_count = 1
                #upcoming games may number above 50 entries and can be split across 2+ pages in the ajax call
                while games_upcoming_url_page_count != 0 and page_no <= games_upcoming_url_page_count:
                    games_upcoming_url = f'https://www.gog.com/games/ajax/filtered?availability=coming&mediaType=game&page={page_no}&sort=date'
                    #parse new ids from the games page ajax call
                    games_upcoming_url_page_count = gog_product_games_ajax_query(games_upcoming_url, scan_mode, session, db_connection)
                    page_no += 1
                    
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        pass
        
elif scan_mode == 'manual':
    logger.info('--- Running in MANUAL scan mode ---')
    
    manual_scan_section = configParser['MANUAL_SCAN']
    #load the product id list to process
    product_id_list = [int(product_id.strip()) for product_id in 
                       manual_scan_section.get('id_list').split(',')]
    
    try:
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                for product_id in product_id_list:
                    logger.info(f'Running scan for id {product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete:
                        if retry_counter > 0:
                            logger.warning(f'Reprocessing id {product_id}...')
                            #allow a short respite before re-processing
                            sleep(2)
                            
                        retries_complete = gog_product_extended_query(product_id, scan_mode, session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {product_id}.')       
                        else:
                            retry_counter += 1
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
    
    except KeyboardInterrupt:
        pass
    
#run product scans against adalia fundamentals tracked ids and potentially other third party lists
elif scan_mode == 'third_party':
    logger.info('--- Running in THIRD PARTY scan mode ---')
    
    try:
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                #gracefully provided by adaliabooks
                #ids which are probably bundeled - adalia fundamentals
                gog_products_third_party_query(ADALIA_MISSING_URL, scan_mode, session, db_connection)
                #all the ids in the catalog - adalia fundamentals
                gog_products_third_party_query(ADALIA_LEGACY_URL, scan_mode, session, db_connection)
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        pass
    
#extract file entries collected during the latest update runs
elif scan_mode == 'extract':
    logger.info('--- Running in FILE EXTRACT scan mode ---')
    
    try:
        logger.info('Starting files scan on all existing DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_int_delisted IS NULL ORDER BY 1')
            id_list = db_cursor.fetchall()
            logger.debug('Retrieved all existing product ids from the DB...')
            
            for id_entry in id_list:
                current_product_id = id_entry[0]
                logger.debug(f'Now processing id {current_product_id}...')
                
                gog_files_extract_parser(db_connection, current_product_id)
                        
            logger.debug('Running PRAGMA optimize...')
            db_connection.execute(OPTIMIZE_QUERY)
            
    except KeyboardInterrupt:
        pass
    
elif scan_mode == 'delisted':
    logger.info('--- Running in DELISTED scan mode ---')
    
    try:
        logger.info('Starting scan on all delisted DB entries...')
        
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                #select all products which are no longer listed, excluding potential duplicates
                db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_int_delisted IS NOT NULL ORDER BY 1')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all delisted product ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete:
                        if retry_counter > 0:
                            logger.warning(f'Reprocessing id {current_product_id}...')
                            #allow a short respite before re-processing
                            sleep(2)

                        retries_complete = gog_product_extended_query(current_product_id, scan_mode, session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                        else:
                            retry_counter += 1
                            
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
    
    except KeyboardInterrupt:
        pass

if scan_mode == 'update' and reset_id:
    logger.info('Resetting last_id parameter...')
    configParser.read(conf_file_full_path)
    configParser['UPDATE_SCAN']['last_id'] = '0'
                    
    with open(conf_file_full_path, 'w') as file:
        configParser.write(file)

logger.info('All done! Exiting...')
        
##main thread end
