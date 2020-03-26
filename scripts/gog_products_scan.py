#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 1.30
@date: 23/03/2020

Warning: Built for use with python 3.6+
'''

import json
import html
import sqlite3
import numpy
import requests
import logging
import argparse
from logging.handlers import RotatingFileHandler
from sys import argv
from shutil import copy2
from configparser import ConfigParser
from html2text import html2text
from datetime import datetime
from os import path
from time import sleep
from collections import OrderedDict
from lxml import html as lhtml

##global parameters init
configParser = ConfigParser()
terminate_signal = False
reset_id = True

##conf file block
conf_file_full_path = path.join('..', 'conf', 'gog_products_scan.conf')

##logging configuration block
log_file_full_path = path.join('..', 'logs', 'gog_products_scan.log')
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
INSERT_ID_QUERY = 'INSERT INTO gog_products VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

UPDATE_ID_QUERY =  ('UPDATE gog_products SET gp_int_previous_update = ?, '
                        'gp_int_latest_update = ?, '
                        'gp_int_no_longer_listed = ?, '
                        'gp_int_previous_full_json_payload = ?, '
                        'gp_int_full_json_payload = ?, '
                        'gp_title = ?, '
                        'gp_slug = ?, '
                        'gp_cs_compat_windows = ?, '
                        'gp_cs_compat_osx = ?, '
                        'gp_cs_compat_linux = ?, '
                        'gp_languages = ?, '
                        'gp_links_forum = ?, '
                        'gp_links_product_card = ?, '
                        'gp_links_purchase_link = ?, '
                        'gp_links_support = ?, '
                        'gp_in_development_active = ?, '
                        'gp_in_development_until = ?, '
                        'gp_is_secret = ?, '
                        'gp_is_installable = ?, '
                        'gp_game_type = ?, '
                        'gp_is_pre_order = ?, '
                        'gp_release_date = ?, '
                        'gp_description_lead = ?, '
                        'gp_description_full = ?, '
                        'gp_description_cool = ?, '
                        'gp_changelog = ? WHERE gp_id = ?')

COMPANY_SELECT_FILTER_QUERY = ('SELECT gc_int_nr FROM gog_companies WHERE '
                                'upper('
                                    'replace('
                                        'replace('
                                            'replace('
                                                'replace('
                                                    'replace('
                                                        'replace(gc_name,".",""),'
                                                    '":",""),'
                                                '"/ ",""),'
                                            '", ",""),'
                                        '"/",""),'
                                    '",","")'
                                ') = ?')

INSERT_FILES_QUERY = 'INSERT INTO gog_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

ADALIA_MISSING_URL = 'https://gog.bigpizzapies.com/missingUrls.json'
ADALIA_LEGACY_URL = 'https://gog.bigpizzapies.com/legacyUrls.json'

#set the gog_lc cookie to avoid errors bought about by GOG dynamically determining the site language
COOKIES = {
    'gog_lc': 'BE_EUR_en-US'
}

def gog_process_json_payload(json_payload):
    values_pretty = [json.dumps(item, sort_keys=True, ensure_ascii=False) for item in json_payload.values()]
    
    #uncomment for debugging of JSON payload
    #for item in values_pretty:
    #   logger.debug(item)
    
    '''ye' olde indexed map of all primary fields currently returned part of the json payload:
        [0]  - id
        [1]  - title
        [2]  - purchase_link
        [3]  - slug
        [4]  - content_system_compatibility
        [5]  - languages
        [6]  - links
        [7]  - in_development
        [8]  - is_secret
        [9]  - is_installable
        [10]  - game_type
        [11] - is_pre_order
        [12] - release_date
        [13] - images
        [14] - dlcs
        [15] - downloads
        [16] - expanded_dlcs
        [17] - description
        [18] - screenshots
        [19] - videos
        [20] - related_products
        [21] - changelog
    '''
    
    #remove values which will not be stored explicitly in the database:
    del values_pretty[20] #related_products
    del values_pretty[19] #videos
    del values_pretty[18] #screenshots
    del values_pretty[16] #expanded_dlcs
    del values_pretty[15] #downloads
    del values_pretty[14] #dlcs
    del values_pretty[13] #images
    del values_pretty[2]  #purchase_link
    
    '''ye' olde indexed map of all primary fields currently returned part of the json payload (after removals):
        [0]  - id
        [1]  - title
        [2]  - slug
        [3]  - content_system_compatibility
        [4]  - languages
        [5]  - links
        [6]  - in_development
        [7]  - is_secret
        [8]  - is_installable
        [9]  - game_type
        [10] - is_pre_order
        [11] - release_date
        [12] - description             
        [13] - changelog
    '''
    
    #process content_system_compatibility
    content_system_compatibility = json.loads(values_pretty[3])
    del values_pretty[3]
    cs_compat_windows = json.dumps(content_system_compatibility['windows'], ensure_ascii=False)
    cs_compat_osx = json.dumps(content_system_compatibility['osx'], ensure_ascii=False)
    cs_compat_linux = json.dumps(content_system_compatibility['linux'], ensure_ascii=False)
    values_pretty.insert(3, cs_compat_windows)
    values_pretty.insert(4, cs_compat_osx)
    values_pretty.insert(5, cs_compat_linux)
    
    '''ye' olde indexed map of all primary fields currently returned part of the json payload (after content_system_compatibility):
        [0]  - id
        [1]  - title
        [2]  - slug
        [3]  - cs_compat_windows
        [4]  - cs_compat_osx
        [5]  - cs_compat_linux
        [6]  - languages
        [7]  - links
        [8]  - in_development
        [9]  - is_secret
        [10] - is_installable
        [11] - game_type
        [12] - is_pre_order
        [13] - release_date
        [14] - description             
        [15] - changelog
    '''
    #process languages
    languages = json.loads(values_pretty[6])
    del values_pretty[6]
    languages_processed = json.dumps(languages, ensure_ascii=False)
    #remove list headers, at they are not needed in the DB table
    languages_processed = languages_processed.replace('{','').replace('}', '')
    values_pretty.insert(6, languages_processed)
    
    '''ye' olde indexed map of all primary fields currently returned part of the json payload (after languages):
        [0]  - id
        [1]  - title
        [2]  - slug
        [3]  - cs_compat_windows
        [4]  - cs_compat_osx
        [5]  - cs_compat_linux
        [6]  - languages_processed
        [7]  - links
        [8]  - in_development
        [9]  - is_secret
        [10] - is_installable
        [11] - game_type
        [12] - is_pre_order
        [13] - release_date
        [14] - description             
        [15] - changelog
    '''
    
    #process links
    links = json.loads(values_pretty[7])
    del values_pretty[7]
    links_forum = json.dumps(links['forum'], ensure_ascii=False)
    links_product_card = json.dumps(links['product_card'], ensure_ascii=False)
    links_purchase_link = json.dumps(links['purchase_link'], ensure_ascii=False)
    links_support = json.dumps(links['support'], ensure_ascii=False)
    values_pretty.insert(7, links_forum)
    values_pretty.insert(8, links_product_card)
    values_pretty.insert(9, links_purchase_link)
    values_pretty.insert(10, links_support)
    
    '''ye' olde indexed map of all primary fields currently returned part of the json payload (after links):
        [0]  - id
        [1]  - title
        [2]  - slug
        [3]  - cs_compat_windows
        [4]  - cs_compat_osx
        [5]  - cs_compat_linux
        [6]  - languages_processed
        [7]  - links_forum
        [8]  - links_product_card
        [9]  - links_purchase_link
        [10] - links_support
        [11] - in_development
        [12] - is_secret
        [13] - is_installable
        [14] - game_type
        [15] - is_pre_order
        [16] - release_date
        [17] - description             
        [18] - changelog
    '''
        
    #process in_development
    in_development = json.loads(values_pretty[11])
    del values_pretty[11]
    in_development_active = json.dumps(in_development['active'], ensure_ascii=False)
    in_development_until = json.dumps(in_development['until'], ensure_ascii=False)
    values_pretty.insert(11, in_development_active)
    values_pretty.insert(12, in_development_until)
    
    '''ye' olde indexed map of all primary fields currently returned part of the json payload (after in_development):
        [0]  - id
        [1]  - title
        [2]  - slug
        [3]  - cs_compat_windows
        [4]  - cs_compat_osx
        [5]  - cs_compat_linux
        [6]  - languages_processed
        [7]  - links_forum
        [8]  - links_product_card
        [9]  - links_purchase_link
        [10] - links_support
        [11] - in_development_active
        [12] - in_development_until
        [13] - is_secret
        [14] - is_installable
        [15] - game_type
        [16] - is_pre_order
        [17] - release_date
        [18] - description             
        [19] - changelog
    '''
    
    #process description
    description = json.loads(values_pretty[-2])
    del values_pretty[-2]
    description_lead = html2text(json.dumps(description['lead'], ensure_ascii=False).replace('\\n', '\n')).strip()
    description_full = html2text(json.dumps(description['full'], ensure_ascii=False).replace('\\n', '\n')).strip()
    description_cool = html2text(json.dumps(description['whats_cool_about_it'], ensure_ascii=False).replace('\\n', '\n')).strip()
    values_pretty.insert(-1, description_lead)
    values_pretty.insert(-1, description_full)
    values_pretty.insert(-1, description_cool)
    
    '''ye' olde indexed map of all primary fields currently returned part of the json payload (after description):
        [0]  - id
        [1]  - title
        [2]  - slug
        [3]  - cs_compat_windows
        [4]  - cs_compat_osx
        [5]  - cs_compat_linux
        [6]  - languages_processed
        [7]  - links_forum
        [8]  - links_product_card
        [9]  - links_purchase_link
        [10] - links_support
        [11] - in_development_active
        [12] - in_development_until
        [13] - is_secret
        [14] - is_installable
        [15] - game_type
        [16] - is_pre_order
        [17] - release_date
        [18] - description_lead
        [19] - description_full
        [20] - description_cool
        [21] - changelog
    '''
    
    #process changelog
    changelog = values_pretty[-1]
    del values_pretty[-1]
    changelog_text = html2text(changelog.replace('\\n', '')).strip()
    #correct some html2text wierdness by removing whitespace via indexes
    values_pretty.append(changelog_text[3:-2])
    
    '''ye' olde indexed map of all primary fields currently returned part of the json payload (after description):
        [0]  - id
        [1]  - title
        [2]  - slug
        [3]  - cs_compat_windows
        [4]  - cs_compat_osx
        [5]  - cs_compat_linux
        [6]  - languages_processed
        [7]  - links_forum
        [8]  - links_product_card
        [9]  - links_purchase_link
        [10] - links_support
        [11] - in_development_active
        [12] - in_development_until
        [13] - is_secret
        [14] - is_installable
        [15] - game_type
        [16] - is_pre_order
        [17] - release_date
        [18] - description_lead
        [19] - description_full
        [20] - description_cool
        [21] - changelog
    '''
    
    #filter regular values by removing double quotes and 'null' values in other 
    #programming languages in order to store actual 'NULL' in the SQLite database
    #and remove any trailing or leading whitespace
    array_index = 0
    for item in values_pretty:
        if item is not None:
            if item == 'null' or item.strip() == '' or item.strip() == '""' or item.strip() == "''":
                item = None
            else:
                #remove JSON enforced double quotes, as they are not needed in the DB
                #and remove any trailing or leading whitespace
                item = item.replace('"','').strip() 
            values_pretty[array_index] = item
        array_index += 1
        
    return values_pretty

def gog_product_company_query(product_id, product_url, scan_mode, session):
    #convert to https, as some product_card urls can be http-based
    if product_url.find('http://') != -1:
        product_url = product_url.replace('http://', 'https://')
    
    try:
        response = session.get(product_url, cookies=COOKIES, timeout=300)
        
        logger.debug(f'CQ >>> HTTP response code: {response.status_code}')
        
        if response.status_code == 200 and response.text is not None and response.text.find('"error": "server_error"') == -1:
            logger.debug(f'CQ >>> HTTP response URL: {response.url}')
            
            #check if the response URL remained identical to with the provided product URL
            if response.url == product_url:
                if scan_mode == 'manual':
                    logger.info(f'CQ >>> Product company query for id {product_id} has returned a valid response...')
                    
                html_tree = lhtml.fromstring(response.text)
                parent_divs = html_tree.xpath('//div[contains(@class, "table__row")]/div[contains(@class, "details__category") and contains(@class, "table__row-label")]/text()')
                #strip any detected values to fix occasional GOG paging whitespace
                parent_divs = [item.strip() for item in parent_divs]
                logger.debug(f'CQ >>> Found parent elements value: {parent_divs}')
                
                #check if the 'Company:' tag is present among the parent divs
                if 'Company:' in parent_divs:
                    #developer_raw = html_tree.xpath('//div[@class="table__row-content"]/a[@class="details__link ng-scope" and contains(@eventLabel, "Developer:")]/text()')
                    developer_raw = html_tree.xpath('//div[contains(@class, "details__content") and contains(@class, "table__row-content")]'
                                                    '/a[@class="details__link" and contains(@gog-track-event, "eventLabel: \'Developer:")]/text()')[0]
                    logger.debug(f'CQ >>> Found developer raw value: {developer_raw}')
                    #unescape any potentially remanent HTML notations such as '&amp;'
                    developer = html.unescape(developer_raw.strip())
                    
                    publisher_raw = html_tree.xpath('//div[contains(@class, "details__content") and contains(@class, "table__row-content")]'
                                                    '/a[@class="details__link" and contains(@gog-track-event, "eventLabel: \'Publisher:")]/text()')[0]
                    logger.debug(f'CQ >>> Found publisher raw value: {publisher_raw}')
                    #unescape any potentially remanent HTML notations such as '&amp;'
                    publisher = html.unescape(publisher_raw.strip())
                    
                    return(developer, publisher)
                else:
                    if scan_mode == 'manual':
                        logger.warning('CQ >>> Unable to find a valid company div section. Perhaps the product is no longer being sold?')
                    return(None, None)
            
            #invalid product URLs will redirect to the GOG games page
            else:
                if scan_mode == 'manual':
                    logger.warning('CQ >>> Product URL has been redirected to the GOG games page. Perhaps the product is no longer being sold?')
                return(None, None)
            
        elif response.status_code == 200 and response.text is not None and response.text.find('"error": "server_error"') != -1:
            logger.error('CQ >>> Non-HTTP server-side exception received.')
            raise Exception()
        
        #this should not happen (ever)
        elif response.status_code == 200 and response.text is None:
            logger.error('CQ >>> Received a null HTTP response text.')
            raise Exception()
        
        #response.status_code != 200
        else:
            logger.debug(f'CQ >>> HTTP error code received: {response.status_code}.')
            raise Exception()
        
    except:
        logger.error(f'CQ >>> Product company query has failed for {product_id}!')
        raise
    
def gog_product_extended_query(product_id, scan_mode, session, db_connection):
    
    product_url = f'https://api.gog.com/products/{product_id}?expand=downloads,expanded_dlcs,description,screenshots,videos,related_products,changelog'
    
    try:
        response = session.get(product_url, timeout=300)
            
        logger.debug(f'PQ >>> HTTP response code: {response.status_code}')
            
        if response.status_code == 200 and response.text is not None and response.text != '[]' and response.text.find('"error": "server_error"') == -1:
            if scan_mode == 'manual':
                logger.info(f'PQ >>> Product query for id {product_id} has returned a valid response...')
            
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            json_pretty = json.dumps(json_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
            values_pretty = gog_process_json_payload(json_parsed)
                
            cursor = db_connection.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM gog_products WHERE gp_id = ?', [int(product_id), ])
            entry_count = cursor.fetchone()[0]
            
            #no need to do any advanced processing if an entry is found in 'manual' scan mode,
            #since that entry will be skipped anyway
            if not (entry_count == 1 and scan_mode == 'manual'):
                '''ye' olde indexed map of all primary fields currently returned part of the json payload (after description):
                    [0]  - id
                    [1]  - title
                    [2]  - slug
                    [3]  - cs_compat_windows
                    [4]  - cs_compat_osx
                    [5]  - cs_compat_linux
                    [6]  - languages_processed
                    [7]  - links_forum
                    [8]  - links_product_card
                    [9]  - links_purchase_link
                    [10] - links_support
                    [11] - in_development_active
                    [12] - in_development_until
                    [13] - is_secret
                    [14] - is_installable
                    [15] - game_type
                    [16] - is_pre_order
                    [17] - release_date
                    [18] - description_lead
                    [19] - description_full
                    [20] - description_cool
                    [21] - changelog
                '''
                
                #if the API returned product title starts with 'product_title_', keep the existing product title
                if values_pretty[1] is not None and values_pretty[1].startswith('product_title_'):
                    logger.warning('PQ >>> Product title update skipped since an invalid value was returned!')
                    cursor.execute('SELECT gp_title FROM gog_products WHERE gp_id = ?', [int(product_id), ])
                    #to be used for all loggers
                    product_title = cursor.fetchone()[0]
                else:
                    #to be used for all loggers
                    product_title = values_pretty[1]
                
                ##movie detection logic
                
                #detect if the entry is a movie based on the content of the lead description field
                #I know, not very pretty, but hey, it works
                if values_pretty[18] is not None:
                    if(values_pretty[18].startswith('IMDB rating:') or
                       values_pretty[18].startswith('Duration:') or
                       #fix for Super Game Jam - technically a movie though it provides some games as bonus
                       values_pretty[18].startswith('**Includes 5 short films & 5 short games.**') or
                       #fix for Deliverance: The Making of Kingdom Come - may want to think of a more elegant way to do this
                       values_pretty[18].startswith('Also available:')):
                        is_movie = 'true'
                    else:
                        is_movie = 'false'
                #when no description is provided, consider the entry a non-movie entry
                else:
                    is_movie = 'false'
                        
                #be a pessimist by default
                dev_pub_null = 'true'
                
                ##company query call
                product_card = json_parsed['links']['product_card']
                if product_card is not None and product_card != '':
                    developer, publisher = gog_product_company_query(product_id, product_card, scan_mode, session)
                else:
                    if scan_mode == 'manual':
                        logger.warning('PQ >>> Product company query skipped since a null product card value was returned!')
                    developer = None
                    publisher = None
                
                ##determine developer/publisher logic
                if developer is not None:
                    dev_pub_null = 'false'
                    
                    cursor.execute('SELECT gc_int_nr FROM gog_companies WHERE gc_name = ?', [developer, ])
                    developer_fk_array = cursor.fetchone()
                    if developer_fk_array is not None:
                        developer_fk = developer_fk_array[0]
                    else:
                        #also try to match the uppercase variant with some filtering of the developer 
                        #(works in some case, for ex: LTD. vs Ltd., 1C:Ino-Co vs 1C Ino-Co etc)
                        cursor.execute(COMPANY_SELECT_FILTER_QUERY, 
                                       [developer.upper().replace('.','').replace(':','')
                                        .replace(', ','').replace('/ ','').replace(',','').replace('/',''), ])
                        developer_fk_array = cursor.fetchone()
                        
                        if developer_fk_array is not None:
                            developer_fk = developer_fk_array[0]
                        else:
                            logger.warning(f'PQ >>> Unable to link developer name to an existing DB company entry for {product_id}!')
                            developer_fk = None
                else:
                    developer_fk = None
                    
                if publisher is not None:
                    dev_pub_null = 'false'
                    
                    cursor.execute('SELECT gc_int_nr FROM gog_companies WHERE gc_name = ?', [publisher, ])
                    publisher_fk_array = cursor.fetchone()
                    if publisher_fk_array is not None:
                        publisher_fk = publisher_fk_array[0]
                    else:
                        #also try to match the uppercase variant with some filtering of the publisher 
                        #(works in some case, for ex: LTD. vs Ltd., 1C:Ino-Co vs 1C Ino-Co etc)
                        cursor.execute(COMPANY_SELECT_FILTER_QUERY, 
                                       [publisher.upper().replace('.','').replace(':','')
                                        .replace(', ','').replace('/ ','').replace(',','').replace('/',''), ])
                        publisher_fk_array = cursor.fetchone()
                        
                        if publisher_fk_array is not None:
                            publisher_fk = publisher_fk_array[0]
                        else:
                            logger.warning(f'PQ >>> Unable to link publisher name to an existing DB company entry for {product_id}!')
                            publisher_fk = None
                else:
                    publisher_fk = None
            
            if entry_count == 0:
                #add custom db field values to the HTTP response list
                #gp_int_nr
                values_pretty.insert(0, None)
                #gp_int_added
                values_pretty.insert(1, str(datetime.now()))
                #gp_int_previous_update
                values_pretty.insert(2, None)
                #gp_int_no_longer_listed
                values_pretty.insert(3, None)
                #gp_int_dev_pub_null
                values_pretty.insert(4, dev_pub_null)
                #gp_int_latest_update
                values_pretty.insert(5, None)
                #gp_int_is_movie
                values_pretty.insert(6, is_movie)
                #gp_int_product_url
                values_pretty.insert(7, product_url)
                #gp_int_previous_full_json_payload
                values_pretty.insert(8, None)
                #gp_int_full_json_payload
                values_pretty.insert(9, json_pretty)
                #gp_developer
                values_pretty.insert(13, developer)
                #gp_publisher
                values_pretty.insert(14, publisher)
                #gp_developer_fk
                values_pretty.insert(15, developer_fk)
                #gp_publisher_fk
                values_pretty.insert(16, publisher_fk)

                cursor.execute(INSERT_ID_QUERY, numpy.array(values_pretty).tolist())
                db_connection.commit()

                logger.info(f'PQ +++ Added a new DB entry for {product_id}: {product_title}')
            
            elif entry_count == 1:
                #do not update existing entries in a full scan, since update/delta scans will take care of that
                if scan_mode == 'manual':
                    logger.info(f'PQ >>> Found an existing db entry with id {product_id}. Skipping.')
                else:
                    cursor.execute('SELECT gp_int_dev_pub_null, gp_int_full_json_payload, gp_int_latest_update, gp_developer, gp_publisher FROM gog_products WHERE gp_id = ?', [int(product_id), ])
                    query_result = cursor.fetchone()
                    existing_dev_pub_null = query_result[0]
                    existing_full_json = query_result[1]
                    existing_update_timestamp = query_result[2]
                    existing_developer = query_result[3]
                    existing_publisher = query_result[4]
                    
                    if existing_full_json != json_pretty or existing_developer != developer or existing_publisher != publisher:
                        if existing_developer != developer or existing_publisher != publisher:
                            if developer is not None and publisher is not None and developer != '' and publisher != '':
                                logger.info(f'PQ >>> Developer/publisher is out of date for {product_id}. Updating...')
                                cursor.execute('UPDATE gog_products SET gp_developer = ?, gp_publisher = ?, gp_developer_fk = ?, gp_publisher_fk = ?, gp_int_dev_pub_null = ? WHERE gp_id = ?', 
                                               [developer, publisher, developer_fk, publisher_fk, dev_pub_null, int(product_id)])
                                db_connection.commit()
                                logger.info(f'PQ ~~~ Successfully updated developer/publisher for {product_id}: {product_title}')
                            else:
                                dev_pub_null = 'true'
                                #only log warning and update if the developer/publisher null status has just changed from false to true
                                if existing_dev_pub_null == 'false':
                                    logger.warning(f'PQ >>> Current developer/publisher for {product_id} is null. Will retain previous values!')
                                    
                                    cursor.execute('UPDATE gog_products SET gp_int_dev_pub_null = ? WHERE gp_id = ?', 
                                               [dev_pub_null, int(product_id)])
                                    db_connection.commit()
                                    logger.info(f'PQ ~~~ Successfully updated developer/publisher null status for {product_id}: {product_title}')
                        
                        if existing_full_json != json_pretty:
                            logger.info(f'PQ >>> Existing entry for {product_id} is out of date. Updating...')
                            
                            #add custom db field values to the HTTP response list
                            #gp_int_pervious_update
                            values_pretty.insert(0, existing_update_timestamp)
                            #gp_int_latest_update
                            values_pretty.insert(1, str(datetime.now()))
                            #gp_int_no_longer_listed
                            values_pretty.insert(2, None)
                            #gp_int_previous_full_json_payload
                            values_pretty.insert(3, existing_full_json)
                            #gp_int_full_json_payload
                            values_pretty.insert(4, json_pretty)
                            #add gp_id at the bottom of the list
                            values_pretty.append(values_pretty[5])
                            #remove gp_id from initial position
                            del values_pretty[5]
                            cursor.execute(UPDATE_ID_QUERY, numpy.array(values_pretty).tolist())
                            db_connection.commit()
                            logger.info(f'PQ ~~~ Updated the DB entry for {product_id}: {product_title}')
            
            cursor.close()
                
        elif scan_mode == 'update' and response.status_code == 200 and response.text is not None and response.text == '[]':
            logger.debug(f'PQ >>> Product with id {product_id} returned a blank list ("[]").')
            cursor = db_connection.cursor()
            #check to see the existing value for gp_int_no_longer_listed
            cursor.execute('SELECT gp_int_no_longer_listed FROM gog_products WHERE gp_id = ?', [int(product_id), ])
            current_no_longer_listed = cursor.fetchone()[0]
            
            #only alter the entry of not already marked as no longer listed
            if current_no_longer_listed is None:
                logger.warning(f'PQ >>> Product with id {product_id} is no longer listed...')
                cursor.execute('UPDATE gog_products SET gp_int_no_longer_listed = ?  WHERE gp_id = ?', [str(datetime.now()), int(product_id)])
                db_connection.commit()
                logger.info(f'PQ --- Updated the DB entry for: {product_id}')
                cursor.close()
            else:
                cursor.close()
                logger.info(f'PQ >>> Product with id {product_id} is already marked as no longer listed.')
        
        elif scan_mode == 'manual' and response.status_code == 200 and response.text is not None and response.text == '[]':
            logger.debug(f'PQ >>> Product with id {product_id} returned a blank list ("[]"). Skipping.')
            
        elif response.status_code == 200 and response.text is not None and response.text.find('"error": "server_error"') != -1:
            logger.error(f'PQ >>> Product with id {product_id} returned a Non-HTTP server-side exception')
            raise Exception()
                    
        #some ids apparently return a 404 HTTP error code on removal
        elif scan_mode == 'update' and response.status_code == 404:
            cursor = db_connection.cursor()
            #check to see the existing value for gp_int_no_longer_listed
            cursor.execute('SELECT gp_int_no_longer_listed FROM gog_products WHERE gp_id = ?', [int(product_id), ])
            current_no_longer_listed = cursor.fetchone()[0]
            
            #only alter the entry of not already marked as no longer listed
            if current_no_longer_listed is None:
                logger.warning(f'PQ >>> Product with id {product_id} is no longer listed...')
                cursor.execute('UPDATE gog_products SET gp_int_no_longer_listed = ? WHERE gp_id = ?', [str(datetime.now()), int(product_id)])
                db_connection.commit()
                logger.info(f'PQ --- Updated the DB entry for: {product_id}')
                cursor.close()
            else:
                cursor.close()
                logger.info(f'PQ >>> Product with id {product_id} is already marked as no longer listed.')
                    
        elif scan_mode == 'manual' and response.status_code == 404:
            logger.debug(f'PQ >>> Product with id {product_id} returned a HTTP 404 error code. Skipping.')
            
        elif scan_mode == 'update' and (response.status_code == 429 or response.status_code == 509):
            logger.critical('PQ >>> 429 or 509 HTTP error code received. Please abort process!')
            raise Exception()
        
        #this should not happen (ever)
        elif response.status_code == 200 and response.text is None:
            logger.error('PQ >>> Received a null HTTP response text.')
            raise Exception()
        
        #response.status_code != 200
        else:
            logger.debug(f'PQ >>> HTTP error code received: {response.status_code}.')
            raise Exception()
        
    except:
        logger.error(f'PQ >>> Product extended query has failed for {product_id}!')
        raise
    
def gog_product_games_ajax_query(url, scan_mode, session, db_connection):
    
    logger.info(f'GQ >>> Querying url: {url}')
    
    #return a value of 1, should something go terribly wrong
    totalPages = 1
    
    try:
        #reuse session connection(s) to send a GET request
        response = session.get(url, cookies=COOKIES, timeout=300)
        
        logger.debug(f'GQ >>> HTTP response code: {response.status_code}')
        
        if response.status_code == 200 and response.text is not None and response.text.find('"error": "server_error"') == -1:
            gogData_json = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            #return the total number of pages, as listed in the response
            totalPages = gogData_json['totalPages']
            logger.debug(f'GQ >>> Total pages: {totalPages}')
                        
            #use a set to avoid processing potentially duplicate ids
            id_set = set()
            
            for product_element in gogData_json['products']:
                id_value = product_element['id']
                logger.debug(f'GQ >>> Found the following id: {id_value}')
                id_set.add(id_value)
            
            #transform the set to a list in order to be able to sort it
            id_list = list(id_set)
            id_list.sort()
                
            for product_id in id_list:
                complete = False
                logger.debug(f'GQ >>> Running scan for id {product_id}...')
                retry_counter = 0
                
                while not complete:
                    if retry_counter > 0:
                        #allow a short respite before re-processing
                        sleep(2)
                        logger.warning(f'GQ >>> Reprocessing id {product_id}...')
                                  
                    try:
                        gog_product_extended_query(product_id, scan_mode, session, db_connection)
                        complete = True
                    except:
                        complete = False
                        retry_counter += 1
                        #uncomment for debugging purposes only
                        #raise
        
        elif response.status_code == 200 and response.text is not None and response.text.find('"error": "server_error"') != -1:
            logger.error('GQ >>> Non-HTTP server-side exception received.')
            raise Exception()
            
        #this should not happen (ever)
        elif response.status_code == 200 and response.text is None:
            logger.error('GQ >>> Received a null HTTP response text.')
            raise Exception()
             
        #response.status_code != 200
        else:
            logger.error(f'GQ >>> HTTP error code received: {response.status_code}.')
            raise Exception()
        
        return totalPages
                
    except:
        logger.error('GQ >>> Processing has failed!')
        #uncomment for debugging purposes only
        #raise
    
def gog_products_third_party_query(third_party_url, scan_mode, session, db_connection):
    
    logger.info(f'TQ >>> Querying url: {third_party_url}')
    
    try:
        #reuse session connection(s) to send a GET request
        response = session.get(third_party_url, timeout=300)
            
        logger.debug(f'TQ >>> HTTP response code: {response.status_code}')
            
        if response.status_code == 200 and response.text is not None:
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            cursor = db_connection.cursor()
            
            for extra_id in json_parsed:
                logger.debug(f'TQ >>> Picked up the following product id: {extra_id}')
                
                #at least one of the ids is null for some reason, so do check
                if extra_id is not None and extra_id != '':
                    cursor.execute('SELECT COUNT(*) FROM gog_products WHERE gp_id = ?', [int(extra_id), ])
                    entry_count = cursor.fetchone()[0]
                    
                    #only run a product scan if a new id was detected
                    if entry_count == 0:
                        logger.debug(f'TQ >>> Unknown id detected! Running scan for {extra_id}')
                        complete = False
                        retry_counter = 0
                            
                        while not complete:
                            if retry_counter > 0:
                                #allow a short respite before re-processing
                                sleep(2)
                                logger.warning(f'TQ >>> Reprocessing id {extra_id}...')
                                    
                            try:
                                gog_product_extended_query(extra_id, scan_mode, session, db_connection)
                                complete = True
                            except:
                                complete = False
                                retry_counter += 1
                                #uncomment for debugging purposes only
                                #raise
                    
                    else:
                        logger.debug('TQ >>> The id is already present in the gog_products table. Skipping!')
                            
            cursor.close()
        
        #this should not happen (ever)
        elif response.status_code == 200 and response.text is None:
            logger.error('TQ >>> Received a null HTTP response text. Aborting!')
            raise Exception()
                 
        #response.status_code != 200
        else:
            logger.error(f'TQ >>> New HTTP error code received: {response.status_code}. Aborting!')
            raise Exception()
    
    except:
        logger.error('TQ >>> Processing has failed!')
        #uncomment for debugging purposes only
        #raise
        
def gog_files_extract_parser(db_connection, product_id):
    
    db_cursor = db_connection.cursor()
    
    db_cursor.execute('SELECT gp_title, gp_int_full_json_payload FROM gog_products WHERE gp_id = ?', [product_id, ])
    #retrieve the product_name and the latest json_payload
    result_row = db_cursor.fetchone()
    product_name = result_row[0]
    json_payload = result_row[1]
    
    #extract installer entries
    json_parsed_installers = json.loads(json_payload, object_pairs_hook=OrderedDict)['downloads']['installers']
    #extract patch entries
    json_parsed_patches = json.loads(json_payload, object_pairs_hook=OrderedDict)['downloads']['patches']
    
    #process installer entries
    for installer_entry in json_parsed_installers:
        installer_id = installer_entry['id']
        installer_product_name = installer_entry['name']
        installer_os = installer_entry['os']
        installer_language = installer_entry['language']
        installer_version = installer_entry['version']
        installer_total_size = installer_entry['total_size']
        
        for installer_file in installer_entry['files']:
            installer_file_id = installer_file['id']
            installer_file_size = installer_file['size']
            installer_file_downlink = installer_file['downlink']
            
            if installer_version is not None:
                db_cursor.execute('SELECT COUNT(gf_id) FROM gog_files WHERE gf_int_product_id = ? and gf_int_type = \'installer\' and gf_id = ? and gf_file_id = ? and gf_os = ? and gf_language = ? and gf_version = ? and gf_file_size = ?', 
                               [product_id, installer_id, installer_file_id, installer_os, installer_language, installer_version, installer_file_size])
            else:
                db_cursor.execute('SELECT COUNT(gf_id) FROM gog_files WHERE gf_int_product_id = ? and gf_int_type = \'installer\' and gf_id = ? and gf_file_id = ? and gf_os = ? and gf_language = ? and gf_version is NULL and gf_file_size = ?', 
                               [product_id, installer_id, installer_file_id, installer_os, installer_language, installer_file_size])
                
            existing_entries = db_cursor.fetchone()[0]
            
            if existing_entries == 0:
                #gf_int_nr, gf_int_added, gf_int_product_id, gf_int_product_name, gf_int_type
                insert_values = [None, str(datetime.now()), product_id, product_name, 'installer']
                #gf_id
                insert_values.append(installer_id)
                #gf_name
                insert_values.append(installer_product_name)
                #gf_os
                insert_values.append(installer_os)
                #gf_language
                insert_values.append(installer_language)
                #gf_version
                insert_values.append(installer_version)
                #gf_total_size
                insert_values.append(installer_total_size)
                #gf_file_id
                insert_values.append(installer_file_id)
                #gf_file_size
                insert_values.append(installer_file_size)
                #gf_file_downlink
                insert_values.append(installer_file_downlink)
                
                db_cursor.execute(INSERT_FILES_QUERY, insert_values)
                
                #no need to print the os here, as it's included in the installer_id
                logger.info(f'FQ +++ Added DB entry for {product_id}: {installer_id}, version_name {installer_version}')
    
    #process patch entries
    for patch_entry in json_parsed_patches:
        patch_id = patch_entry['id']
        patch_product_name = patch_entry['name']
        patch_os = patch_entry['os']
        patch_language = patch_entry['language']
        patch_version = patch_entry['version']
        patch_total_size = patch_entry['total_size']
        
        for patch_file in patch_entry['files']:
            patch_file_id = patch_file['id']
            patch_file_size = patch_file['size']
            patch_file_downlink = patch_file['downlink']
                
            if patch_version is not None:
                db_cursor.execute('SELECT COUNT(gf_id) FROM gog_files WHERE gf_int_product_id = ? and gf_int_type = \'patch\' and gf_id = ? and gf_file_id = ? and gf_os = ? and gf_language = ? and gf_version = ? and gf_file_size = ?', 
                               [product_id, patch_id, patch_file_id, patch_os, patch_language, patch_version, patch_file_size])
            else:
                db_cursor.execute('SELECT COUNT(gf_id) FROM gog_files WHERE gf_int_product_id = ? and gf_int_type = \'patch\' and gf_id = ? and gf_file_id = ? and gf_os = ? and gf_language = ? and gf_version is NULL and gf_file_size = ?', 
                               [product_id, patch_id, patch_file_id, patch_os, patch_language, patch_file_size])
                
            existing_entries = db_cursor.fetchone()[0]
            
            if existing_entries == 0:
                #gf_int_nr, gf_int_added, gf_int_product_id, gf_int_product_name, gf_int_type
                insert_values = [None, str(datetime.now()), product_id, product_name, 'patch']
                #gf_id
                insert_values.append(patch_id)
                #gf_name
                insert_values.append(patch_product_name)
                #gf_os
                insert_values.append(patch_os)
                #gf_language
                insert_values.append(patch_language)
                #gf_version
                insert_values.append(patch_version)
                #gf_total_size
                insert_values.append(patch_total_size)
                #gf_file_id
                insert_values.append(patch_file_id)
                #gf_file_size
                insert_values.append(patch_file_size)
                #gf_file_downlink
                insert_values.append(patch_file_downlink)
                
                db_cursor.execute(INSERT_FILES_QUERY, insert_values)
                
                #no need to print the os here, as it's included in the patch_id
                logger.info(f'FQ +++ Added DB entry for {product_id} patch: {patch_id}, version_name {patch_version}')
                
    #batch commit
    db_connection.commit()

##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description='GOG products scan (part of gog_visor) - a script to call publicly available GOG APIs \
                                              in order to retrieve product information and updates.')

group = parser.add_mutually_exclusive_group()
group.add_argument('-n', '--new', help='Query new products', action='store_true')
group.add_argument('-u', '--update', help='Run an update scan for existing products', action='store_true')
group.add_argument('-m', '--manual', help='Perform a manual products scan', action='store_true')
group.add_argument('-t', '--third_party', help='Perform a third-party (Adalia Fundamentals) products scan', action='store_true')
group.add_argument('-e', '--extract', help='Extract file data from existing products', action='store_true')

args = parser.parse_args()

logger.info('*** Running PRODUCTS scan script ***')

#db file check/backup section
if path.exists(db_file_full_path):
    #create a backup of the existing db - mostly for debugging/recovery
    copy2(db_file_full_path, db_file_full_path + '.bak')
    logger.info('Successfully created db backup.')
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

#detect any parameter overrides and set the scan_mode accordingly
if len(argv) > 1:
    logger.info('Command-line parameter mode override detected.')
    
    if args.new:
        scan_mode = 'new'
    elif args.update:
        scan_mode = 'update'
    elif args.manual:
        scan_mode = 'manual'
    elif args.third_party:
        scan_mode = 'third_party'
    elif args.extract:
        scan_mode = 'extract'
        
if scan_mode == 'update':
    logger.info('--- Running in UPDATE scan mode ---')
    
    #load last processed id
    last_id = int(configParser['UPDATE_SCAN']['last_id'])
    #load last id save frequency
    last_id_save_frequency = int(configParser['UPDATE_SCAN']['last_id_save_frequency'])
    
    if last_id > 0:
        logger.info(f'Restarting update scan from id: {last_id}')
    
    try:
        logger.info('Starting update scan on all existing DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            cursor = db_connection.cursor()
            
            #skip products which are no longer listed
            cursor.execute('SELECT gp_id FROM gog_products WHERE gp_id > ? AND gp_int_no_longer_listed IS NULL ORDER BY 1', [last_id, ])
            array_of_id_lists = cursor.fetchall()
            cursor.close()
            logger.debug('Retrieved all existing product ids from the DB...')
            
            #used to track the number of processed ids
            last_id_counter = 0
                
            with requests.Session() as session:
                for id_list in array_of_id_lists:
                    current_product_id = id_list[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    complete = False
                    retry_counter = 0
                    
                    while not complete:
                        if retry_counter > 0:
                            #allow a short respite before re-processing
                            sleep(2)
                            logger.warning(f'Reprocessing id {current_product_id}...')
                                
                        try:
                            gog_product_extended_query(current_product_id, scan_mode, session, db_connection)
                            complete = True
                            
                            if last_id_counter != 0 and last_id_counter % last_id_save_frequency == 0:
                                configParser.read(conf_file_full_path)
                                configParser['UPDATE_SCAN']['last_id'] = str(current_product_id)
                                
                                with open(conf_file_full_path, 'w') as file:
                                    configParser.write(file)
                                    
                                logger.info(f'Saved scan up to last_id of: {current_product_id}')
                                
                            last_id_counter += 1
                                
                        except KeyboardInterrupt:
                            reset_id = False
                            raise
                        except:
                            complete = False
                            retry_counter += 1
                            #uncomment for debugging purposes only
                            #raise
            
    except KeyboardInterrupt:
        reset_id = False
        pass
    
elif scan_mode == 'new':
    logger.info('--- Running in NEW scan mode ---')
    
    try:
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                page_no = 1
                #start off as 1, then use whatever is returned by the ajax call
                games_new_url_page_count = 1
                #new games may number above 50 entries and can be split across 2+ pages in the ajax call
                while page_no <= games_new_url_page_count:
                    games_new_url = f'https://www.gog.com/games/ajax/filtered?availability=new&mediaType=game&page={page_no}&sort=date'
                    #parse new ids from the games page ajax call
                    games_new_url_page_count = gog_product_games_ajax_query(games_new_url, scan_mode, session, db_connection)
                    page_no += 1
                
                page_no = 1
                #start off as 1, then use whatever is returned by the ajax call
                games_upcoming_url_page_count = 1
                #upcoming games may number above 50 entries and can be split across 2+ pages in the ajax call
                while page_no <= games_upcoming_url_page_count:
                    games_upcoming_url = f'https://www.gog.com/games/ajax/filtered?availability=coming&mediaType=game&page={page_no}&sort=date'
                    #parse new ids from the games page ajax call
                    games_upcoming_url_page_count = gog_product_games_ajax_query(games_upcoming_url, scan_mode, session, db_connection)
                    page_no += 1
            
    except KeyboardInterrupt:
        pass
        
elif scan_mode == 'manual':
    logger.info('--- Running in MANUAL scan mode ---')
    
    #load the product id list to process
    product_id_list = configParser['MANUAL_SCAN']['id_list']
    product_id_list = product_id_list.split(',')
    
    try:
        with requests.Session() as session:
            with sqlite3.connect(db_file_full_path) as db_connection:
                for product_id in product_id_list:
                    logger.info(f'Running scan for id {product_id}')
                    complete = False
                    retry_counter = 0
                    
                    while not complete:
                        if retry_counter > 0:
                            #allow a short respite before re-processing
                            sleep(2)
                            logger.warning(f'Reprocessing id {product_id}...')
                            
                        try:
                            gog_product_extended_query(product_id, scan_mode, session, db_connection)
                            complete = True
                        except:
                            complete = False
                            retry_counter += 1
                            #uncomment for debugging purposes only
                            #raise
            
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
                gog_products_third_party_query(ADALIA_MISSING_URL, 'manual', session, db_connection)
                #all the ids in the catalog - adalia fundamentals
                gog_products_third_party_query(ADALIA_LEGACY_URL, 'manual', session, db_connection)
            
    except KeyboardInterrupt:
        pass
    
#extract file entries collected during the latest update runs
elif scan_mode == 'extract':
    logger.info('--- Running in FILE EXTRACT scan mode ---')
    
    try:
        logger.info('Starting files scan on all existing DB entries...')
        
        with sqlite3.connect(db_file_full_path) as db_connection:
            db_cursor = db_connection.cursor()
                
            db_cursor.execute('SELECT gp_id FROM gog_products ORDER BY 1')
            array_of_id_lists = db_cursor.fetchall()
            db_cursor.close()
            logger.debug('Retrieved all existing product ids from the DB...')
            
            for id_list in array_of_id_lists:
                current_product_id = id_list[0]
                logger.debug(f'Now processing id {current_product_id}...')
                complete = False
                retry_counter = 0
                
                while not complete:
                    if retry_counter > 0:
                        logger.warning(f'Reprocessing id {current_product_id}...')
                            
                    try:
                        gog_files_extract_parser(db_connection, current_product_id)
                        complete = True
                        
                    except KeyboardInterrupt:
                        raise
                    except:
                        complete = False
                        retry_counter += 1
                        #uncomment for debugging purposes only
                        #raise
            
    except KeyboardInterrupt:
        pass

if scan_mode == 'update' and reset_id == True:
    logger.info('Resetting last_id parameter...')
    configParser.read(conf_file_full_path)
    configParser['UPDATE_SCAN']['last_id'] = '0'
                    
    with open(conf_file_full_path, 'w') as file:
        configParser.write(file)

logger.info('All done! Exiting...')
        
##main thread end