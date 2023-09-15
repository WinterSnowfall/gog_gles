#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 3.92
@date: 14/09/2023

Warning: Built for use with python 3.6+
'''

import json
import multiprocessing
import queue
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
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
# uncomment for debugging purposes only
#import traceback

# conf file block
CONF_FILE_PATH = os.path.join('..', 'conf', 'gog_products_scan.conf')
MOVIES_ID_CSV_PATH = os.path.join('..', 'conf', 'gog_products_movie_ids.csv')

# logging configuration block
LOG_FILE_PATH = os.path.join('..', 'logs', 'gog_products_scan.log')
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
INSERT_ID_QUERY = 'INSERT INTO gog_products VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

UPDATE_ID_QUERY = ('UPDATE gog_products SET gp_int_updated = ?, '
                   'gp_int_json_payload = ?, '
                   'gp_int_json_diff = ?, '
                   'gp_title = ?, '
                   'gp_languages = ?, '
                   'gp_changelog = ? WHERE gp_id = ?')

UPDATE_ID_V2_QUERY = ('UPDATE gog_products SET gp_int_v2_updated = ?, '
                      'gp_int_v2_json_payload = ?, '
                      'gp_int_v2_json_diff = ?, '
                      'gp_v2_product_type = ?, '
                      'gp_v2_developer = ?, '
                      'gp_v2_publisher = ?, '
                      'gp_v2_size = ?, '
                      'gp_v2_is_preorder = ?, '
                      'gp_v2_in_development = ?, '
                      'gp_v2_is_installable = ?, '
                      'gp_v2_os_support_windows = ?, '
                      'gp_v2_os_support_linux = ?, '
                      'gp_v2_os_support_osx = ?, '
                      'gp_v2_supported_os_versions = ?, '
                      'gp_v2_global_release_date = ?, '
                      'gp_v2_gog_release_date = ?, '
                      'gp_v2_tags = ?, '
                      'gp_v2_properties = ?, '
                      'gp_v2_series = ?, '
                      'gp_v2_features = ?, '
                      'gp_v2_is_using_dosbox = ?, ' 
                      'gp_v2_links_store = ?, '
                      'gp_v2_links_support = ?, '
                      'gp_v2_links_forum = ?, '
                      'gp_v2_description = ? WHERE gp_id = ?')

INSERT_FILES_QUERY = 'INSERT INTO gog_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

OPTIMIZE_QUERY = 'PRAGMA optimize'

# number of retries after which an id is considered parmenently delisted (for archive mode)
ARCHIVE_NO_OF_RETRIES = 3
# static regex pattern for endline fixing of extra description/changelog whitespace
ENDLINE_FIX_REGEX = re.compile(r'([ ]*[\n]){2,}')
# value separator for multi-valued fields
MVF_VALUE_SEPARATOR = '; '
# supported product OSes, as returned by the v2 API endpoint
SUPPORTED_OSES = ('windows', 'linux', 'osx')
# number of seconds a process will wait to get/put in a queue
QUEUE_WAIT_TIMEOUT = 10 #seconds
# allow a process to fully load before starting the next process 
# (helps preserve process start order)
PROCESS_START_WAIT_INTERVAL = 0.1 #seconds
HTTP_OK = 200
# non-standard unicode values (either encoded or not) which need to be purged from the JSON API output;
# the state of being encoded or not encoded in the original text output seems to depend on some form 
# of unicode string black magic that I can't quite understand...
JSON_UNICODE_FILTERED_VALUES = ('', '\\u0092', '\\u0093', '\\u0094', '\\u0097')

def sigterm_handler(signum, frame):
    # exceptions may happen here as well due to logger syncronization mayhem on shutdown
    try:
        logger.debug('Stopping scan due to SIGTERM...')
    except:
        pass
    
    raise SystemExit(0)

def sigint_handler(signum, frame):
    # exceptions may happen here as well due to logger syncronization mayhem on shutdown
    try:
        logger.debug('Stopping scan due to SIGINT...')
    except:
        pass
    
    raise SystemExit(0)

def parse_html_data(html_content):
    # need to correct some GOG formatting wierdness by using regular expressions
    html_content_parsed = ENDLINE_FIX_REGEX.sub('\n\n', html2text(html_content, bodywidth=0).strip())
    if html_content_parsed == '': html_content_parsed = None
    
    return html_content_parsed

def gog_product_v2_query(process_tag, product_id, db_lock, session, db_connection):
    
    product_url = f'https://api.gog.com/v2/games/{product_id}?locale=en-US'
    
    try:
        response = session.get(product_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'{process_tag}2Q >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == HTTP_OK:
            logger.debug(f'{process_tag}2Q >>> Product v2 query for id {product_id} has returned a valid response...')
            
            # ignore unicode control characters which can be part of game descriptions and/or changelogs; 
            # these chars do absolutely nothing relevant but can mess with SQL imports/export and sometimes 
            # even with unicode conversions from and to the db... why do you do this, GOG, why???
            filtered_response = response.text
            for unicode_value in JSON_UNICODE_FILTERED_VALUES:
                filtered_response = filtered_response.replace(unicode_value, '')

            json_v2_parsed = json.loads(filtered_response, object_pairs_hook=OrderedDict)
            json_v2_formatted = json.dumps(json_v2_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
            
            db_cursor = db_connection.execute('SELECT gp_int_v2_json_payload FROM gog_products WHERE gp_id = ?', (product_id,))
            existing_v2_json_formatted = db_cursor.fetchone()[0]
            
            if existing_v2_json_formatted != json_v2_formatted:
                if existing_v2_json_formatted is not None:
                    logger.debug(f'{process_tag}2Q >>> Existing v2 data for {product_id} is outdated. Updating...')
                
                # calculate the diff between the new json and the previous one
                # (applying the diff on the new json will revert to the previous version)
                if existing_v2_json_formatted is not None:
                    diff_v2_formatted = ''.join([line for line in difflib.unified_diff(json_v2_formatted.splitlines(1), 
                                                                                       existing_v2_json_formatted.splitlines(1), n=0)])
                else:
                    diff_v2_formatted = None
                
                # process product title (for loggers)
                product_title = json_v2_parsed['_embedded']['product']['title'].strip()
                # process product type
                product_type = json_v2_parsed['_embedded']['productType']
                # process developer/publisher
                developer = json_v2_parsed['_embedded']['developers'][0]['name'].strip()
                publisher = json_v2_parsed['_embedded']['publisher']['name'].strip()
                # process size (MB value)
                size = json_v2_parsed['size']
                # process preorder status
                is_preorder = json_v2_parsed['_embedded']['product']['isPreorder']
                # process in development status
                in_development = json_v2_parsed['inDevelopment']['active']
                # process installable status
                is_installable = json_v2_parsed['_embedded']['product']['isInstallable']
                # process individual os support
                supported_oses = json_v2_parsed['_embedded']['supportedOperatingSystems']
                os_support_windows = False
                os_support_linux = False
                os_support_osx = False
                for os_value in supported_oses:
                    if os_value['operatingSystem']['name'] == SUPPORTED_OSES[0]:
                        os_support_windows = True
                    elif os_value['operatingSystem']['name'] == SUPPORTED_OSES[1]:
                        os_support_linux = True
                    elif os_value['operatingSystem']['name'] == SUPPORTED_OSES[2]:
                        os_support_osx = True
                # process supported os versions
                supported_os_versions = MVF_VALUE_SEPARATOR.join(os_value['operatingSystem']['versions'] for os_value in supported_oses 
                                                                 #some ids have empty versions strings for certain oses...
                                                                 if os_value['operatingSystem']['versions'] != '')
                # process global release date
                try:
                    global_release_date = json_v2_parsed['_embedded']['product']['globalReleaseDate']
                except KeyError:
                    global_release_date = None
                # process GOG release date
                gog_release_date = json_v2_parsed['_embedded']['product']['gogReleaseDate']
                # process tags
                tags = MVF_VALUE_SEPARATOR.join(sorted([tag['name'] for tag in json_v2_parsed['_embedded']['tags']]))
                if tags == '': tags = None
                # process properties (tee is used for avoiding a reserved name) - the field may be absent and return a KeyError
                try:
                    # ideally should not need a strip, but there are a few entries with extra whitespace here and there
                    properties = MVF_VALUE_SEPARATOR.join(sorted([propertee['name'].strip() for propertee in 
                                                                  json_v2_parsed['_embedded']['properties']]))
                    if properties == '': properties = None
                except KeyError:
                    properties = None
                # process series - these may be 'null' and return a TypeError
                try:
                    series = json_v2_parsed['_embedded']['series']['name'].strip()
                except TypeError:
                    series = None
                # process features
                features = MVF_VALUE_SEPARATOR.join(sorted([feature['name'] for feature in json_v2_parsed['_embedded']['features']]))
                if features == '': features = None
                # process is_using_dosbox
                is_using_dosbox = json_v2_parsed['isUsingDosBox']
                # proces links
                links_store = json_v2_parsed['_links']['store']['href']
                links_support = json_v2_parsed['_links']['support']['href']
                links_forum = json_v2_parsed['_links']['forum']['href']
                # process description
                try:
                    description = parse_html_data(json_v2_parsed['description'])
                except AttributeError:
                    description = None
                
                with db_lock:
                    # gp_int_v2_updated, gp_int_v2_json_payload, gp_int_v2_previous_json_diff,
                    # gp_v2_product_type, gp_v2_developer, gp_v2_publisher, gp_v2_size,
                    # gp_v2_is_preorder. gp_v2_in_development, gp_v2_is_installable, 
                    # gp_v2_os_support_windows, gp_v2_os_support_linux, gp_v2_os_support_osx, 
                    # gp_v2_supported_os_versions, gp_v2_global_release_date, gp_v2_gog_release_date,
                    # gp_v2_tags, gp_v2_properties, gp_vs_series,
                    # gp_v2_features, gp_v2_is_using_dosbox, 
                    # gp_v2_links_store, gp_v2_links_support, gp_v2_links_forum, 
                    # gp_v2_description, gp_id (WHERE clause)
                    db_cursor.execute(UPDATE_ID_V2_QUERY, (datetime.now(), json_v2_formatted, diff_v2_formatted, 
                                                           product_type, developer, publisher, size,
                                                           is_preorder, in_development, is_installable,
                                                           os_support_windows, os_support_linux, os_support_osx, 
                                                           supported_os_versions, global_release_date, gog_release_date, 
                                                           tags, properties, series, 
                                                           features, is_using_dosbox, 
                                                           links_store, links_support, links_forum, 
                                                           description, product_id))
                    db_connection.commit()
                
                if existing_v2_json_formatted is not None:
                    logger.info(f'{process_tag}2Q ~~~ Updated the v2 data for {product_id}: {product_title}.')
                else:
                    logger.info(f'{process_tag}2Q +++ Added v2 data for {product_id}: {product_title}.')
        
        # ids corresponding to movies will return a 404 error, others should not
        elif response.status_code == 404:
            logger.warning(f'{process_tag}2Q >>> Product with id {product_id} returned a HTTP 404 error code. Skipping.')
        
        else:
            logger.warning(f'{process_tag}2Q >>> HTTP error code {response.status_code} received for {product_id}.')
            raise Exception()
    
    # sometimes the connection may time out
    except requests.Timeout:
        logger.warning(f'{process_tag}2Q >>> HTTP request timed out after {HTTP_TIMEOUT} seconds for {product_id}.')
        raise
    
    except:
        logger.debug(f'{process_tag}2Q >>> Product company query has failed for {product_id}.')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        raise

def gog_product_extended_query(process_tag, product_id, scan_mode, db_lock, session, db_connection):
    # determine if a certain product id can query the v2 endpoint (movies and certain 
    # other ids will not get a valid v2 response, so querying it is useless)
    if product_id in MOVIES_ID_LIST or product_id in NO_V2_ENDPOINT:
        can_query_v2 = False
    else:
        can_query_v2 = True
    
    # there's no need to query the 'description' for regular ids, since it will be contained in the v2 data
    if can_query_v2:
        # unused additional expand options: description, expanded_dlcs, screenshots, videos
        product_url = f'https://api.gog.com/products/{product_id}?expand=downloads,related_products,changelog'
    else:
        # unused additional expand options: expanded_dlcs, screenshots, videos
        product_url = f'https://api.gog.com/products/{product_id}?expand=downloads,description,related_products,changelog'
    
    try:
        response = session.get(product_url, timeout=HTTP_TIMEOUT)
            
        logger.debug(f'{process_tag}PQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == HTTP_OK:
            if scan_mode == 'full' or scan_mode == 'builds':
                logger.info(f'{process_tag}PQ >>> Product query for id {product_id} has returned a valid response...')
            
            db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_products WHERE gp_id = ?', (product_id,))
            entry_count = db_cursor.fetchone()[0]
            
            # no need to do any processing if an entry is found in 'full' or 'builds' scan modes, 
            # since that entry will be skipped anyway
            if not (entry_count == 1 and (scan_mode == 'full' or scan_mode == 'builds')):
                # ignore unicode control characters which can be part of game descriptions and/or changelogs; 
                # these chars do absolutely nothing relevant but can mess with SQL imports/export and sometimes 
                # even with unicode conversions from and to the db... why do you do this, GOG, why???
                filtered_response = response.text
                for unicode_value in JSON_UNICODE_FILTERED_VALUES:
                    filtered_response = filtered_response.replace(unicode_value, '')
                
                json_parsed = json.loads(filtered_response, object_pairs_hook=OrderedDict)
                json_formatted = json.dumps(json_parsed, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
                
                # process unmodified fields
                #product_id = json_parsed['id']
                product_title = json_parsed['title'].strip()
                # process languages
                if len(json_parsed['languages']) > 0:
                    languages = MVF_VALUE_SEPARATOR.join([''.join((language_key, ': ', json_parsed['languages'][language_key])) 
                                                          for language_key in json_parsed['languages'].keys()])
                else:
                    languages = None
                # process changelog
                try:
                    changelog = parse_html_data(json_parsed['changelog'])
                except AttributeError:
                    changelog = None
                
                if can_query_v2:
                    product_type = None
                    gog_release_date = None
                    links_store = None
                    links_support = None
                    links_forum = None
                    description = None
                # change the value of gp_v2_product_type to 'MOVIES' in order to better differentiate them 
                # (it's set to 'GAME' for all movie ids by default, although that makes little sense)
                else:
                    # the value stored here is the lowercase variant of productType in the v2 API payload
                    product_type = 'MOVIE' if product_id in MOVIES_ID_LIST else json_parsed['game_type'].upper()
                    # the value stored here is identical to gogReleaseDate in the v2 API payload
                    gog_release_date = json_parsed['release_date']
                    # the value stored here is identical to store in the v2 API payload
                    links_store = json_parsed['links']['product_card']
                    # the value stored here is identical to support in the v2 API payload
                    links_support = json_parsed['links']['support']
                    # the value stored here is identical to forum in the v2 API payload
                    links_forum = json_parsed['links']['forum']
                    # the value stored here is mostly identical to Description in the v2 API payload
                    try:
                        description = parse_html_data(json_parsed['description']['full'])
                    except AttributeError:
                        description = None
            
            if entry_count == 0:
                with db_lock:
                    # gp_int_nr, gp_int_added, gp_int_delisted, gp_int_updated, gp_int_json_payload, 
                    # gp_int_json_diff, gp_int_v2_updated, gp_int_v2_json_payload, gp_int_v2_json_diff, 
                    # gp_id, gp_title, gp_v2_product_type, gp_v2_developer, gp_v2_publisher, 
                    # gp_v2_size, gp_v2_is_pre_order, gp_v2_in_development, gp_v2_is_installable, 
                    # gp_v2_os_support_windows, gp_v2_os_support_linux, gp_v2_os_support_osx, 
                    # gp_v2_supported_os_versions, gp_v2_global_release_date, gp_v2_gog_release_date, 
                    # gp_v2_tags, gp_v2_properties, gp_v2_series, gp_v2_features, gp_v2_is_using_dosbox, 
                    # gp_v2_links_store, gp_v2_links_support, gp_v2_links_forum,  
                    # gp_v2_description, gp_languages, gp_changelog
                    db_cursor.execute(INSERT_ID_QUERY, (None, datetime.now(), None, None, json_formatted, 
                                                        None, None, None, None, 
                                                        product_id, product_title, product_type, None, None, 
                                                        0, False, False, False, 
                                                        False, False, False, 
                                                        None, None, gog_release_date, 
                                                        None, None, None, None, False, 
                                                        links_store, links_support, links_forum, 
                                                        description, languages, changelog))
                    db_connection.commit()
                logger.info(f'{process_tag}PQ +++ Added a new DB entry for {product_id}: {product_title}.')
                
                if can_query_v2:
                    gog_product_v2_query(process_tag, product_id, db_lock, session, db_connection)
            
            elif entry_count == 1:
                # do not update existing entries in a full or builds scan, since update/delta scans will take care of that
                if scan_mode == 'full' or scan_mode == 'builds':
                    logger.info(f'{process_tag}PQ >>> Found an existing db entry with id {product_id}. Skipping.')
                # manual scans will be treated as update scans
                else:
                    db_cursor.execute('SELECT gp_int_delisted, gp_int_json_payload FROM gog_products WHERE gp_id = ?', (product_id,))
                    existing_delisted, existing_json_formatted = db_cursor.fetchone()
                    
                    # clear the delisted status if an id is relisted (should only happen rarely)
                    if existing_delisted is not None:
                        logger.debug(f'{process_tag}PQ >>> Found a previously delisted entry with id {product_id}. Removing delisted status...')
                        with db_lock:
                            db_cursor.execute('UPDATE gog_products SET gp_int_delisted = NULL WHERE gp_id = ?', (product_id,))
                            db_connection.commit()
                        logger.info(f'{process_tag}PQ *** Removed delisted status for {product_id}: {product_title}.')
                    
                    if existing_json_formatted != json_formatted:
                        logger.debug(f'{process_tag}PQ >>> Existing entry for {product_id} is outdated. Updating...')
                        
                        # calculate the diff between the new json and the previous one
                        # (applying the diff on the new json will revert to the previous version)
                        if existing_json_formatted is not None:
                            diff_formatted = ''.join([line for line in difflib.unified_diff(json_formatted.splitlines(1), 
                                                                                            existing_json_formatted.splitlines(1), n=0)])
                        else:
                            diff_formatted = None
                        
                        with db_lock:
                            # gp_int_updated, gp_int_json_payload, gp_int_json_diff, 
                            # gp_title, gp_languages, gp_changelog, gp_id (WHERE clause)
                            db_cursor.execute(UPDATE_ID_QUERY, (datetime.now(), json_formatted, diff_formatted, 
                                                                product_title, languages, changelog, product_id))
                            db_connection.commit()
                        logger.info(f'{process_tag}PQ ~~~ Updated the DB entry for {product_id}: {product_title}.')
                    
                    if can_query_v2:
                        gog_product_v2_query(process_tag, product_id, db_lock, session, db_connection)
        
        # existing ids return a 404 HTTP error code on removal
        elif scan_mode == 'update' and response.status_code == 404:
            db_cursor = db_connection.execute('SELECT gp_int_delisted, gp_title FROM gog_products WHERE gp_id = ?', (product_id,))
            existing_delisted, product_title = db_cursor.fetchone()
            
            # only alter the entry if not already marked as no longer listed
            if existing_delisted is None:
                logger.debug(f'{process_tag}PQ >>> Product with id {product_id} has been delisted...')
                with db_lock:
                    # also clear diff fields when marking a product as delisted
                    db_cursor.execute('UPDATE gog_products SET gp_int_delisted = ?, gp_int_json_diff = NULL, gp_int_v2_json_diff = NULL '
                                      'WHERE gp_id = ?', (datetime.now(), product_id))
                    db_connection.commit()
                logger.warning(f'{process_tag}PQ --- Delisted the DB entry for: {product_id}: {product_title}.')
            else:
                logger.debug(f'{process_tag}PQ >>> Product with id {product_id} is already marked as delisted.')
        
        # unmapped ids will also return a 404 HTTP error code
        elif response.status_code == 404:
            logger.debug(f'{process_tag}PQ >>> Product with id {product_id} returned a HTTP 404 error code. Skipping.')
        
        else:
            logger.warning(f'{process_tag}PQ >>> HTTP error code {response.status_code} received for {product_id}.')
            raise Exception()
        
        return True
    
    # sometimes the connection may time out
    except requests.Timeout:
        logger.warning(f'{process_tag}PQ >>> HTTP request timed out after {HTTP_TIMEOUT} seconds for {product_id}.')
        return False
    
    # sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'{process_tag}PQ >>> Connection SSL error encountered for {product_id}.')
        return False
    
    # sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'{process_tag}PQ >>> Connection error encountered for {product_id}.')
        return False
    
    except:
        logger.debug(f'{process_tag}PQ >>> Product extended query has failed for {product_id}.')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return False

def gog_product_games_catalog_query(parameters, scan_mode, db_lock, session, db_connection):
    
    catalog_url = f'https://catalog.gog.com/v1/catalog?{parameters}'
    
    logger.debug(f'GQ >>> Querying url: {catalog_url}.')
    
    # return a value of 0, should something go terribly wrong
    pages = 0
    
    try:
        response = session.get(catalog_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'GQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == HTTP_OK:
            gogData_json = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            # return the number of pages, as listed in the response
            pages = gogData_json['pages']
            logger.debug(f'GQ >>> Response pages: {pages}.')
            
            # use a set to avoid processing potentially duplicate ids
            id_set = set()
            
            for product_element in gogData_json['products']:
                id_value = product_element['id']
                logger.debug(f'GQ >>> Found the following id: {id_value}.')
                id_set.add(id_value)
            
            # sort the set into an ordered list
            id_list = sorted(id_set)
            
            for product_id in id_list:
                logger.debug(f'GQ >>> Running scan for id {product_id}...')
                retries_complete = False
                retry_counter = 0
                
                while not retries_complete:
                    if retry_counter > 0:
                        logger.warning(f'GQ >>> Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                        sleep(RETRY_SLEEP_INTERVAL)
                        logger.warning(f'GQ >>> Reprocessing id {product_id}...')
                    
                    retries_complete = gog_product_extended_query('', product_id, scan_mode, db_lock, 
                                                                  session, db_connection)
                    
                    if not retries_complete:
                        retry_counter += 1
                        # terminate the scan if the RETRY_COUNT limit is exceeded
                        if retry_counter > RETRY_COUNT:
                            logger.critical('Retry count exceeded, terminating scan!')
                            raise Exception()
        
        else:
            logger.warning(f'GQ >>> HTTP error code {response.status_code} received.')
            raise Exception()
        
        return (True, pages)
    
    # sometimes the connection may time out
    except requests.Timeout:
        logger.warning(f'GQ >>> HTTP request timed out after {HTTP_TIMEOUT} seconds for {product_id}.')
        return (False, 0)
    
    # sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'GQ >>> Connection SSL error encountered for {product_id}.')
        return (False, 0)
    
    # sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'GQ >>> Connection error encountered for {product_id}.')
        return (False, 0)
    
    except:
        logger.debug('GQ >>> Processing has failed!')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return (False, 0)

def gog_files_extract_parser(db_connection, product_id):
    
    db_cursor = db_connection.execute('SELECT gp_int_json_payload FROM gog_products WHERE gp_id = ?', (product_id,))
    json_payload = db_cursor.fetchone()[0]
    
    json_parsed = json.loads(json_payload, object_pairs_hook=OrderedDict)
    
    # extract installer entries
    json_parsed_installers = json_parsed['downloads']['installers']
    # extract patch entries
    json_parsed_patches = json_parsed['downloads']['patches']
    # extract language_packs entries
    json_parsed_language_packs = json_parsed['downloads']['language_packs']
    # extract bonus_content entries
    json_parsed_bonus_content = json_parsed['downloads']['bonus_content']
    
    # process installer entries
    db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? '
                      'AND gf_int_download_type = \'installer\' AND gf_int_removed IS NULL', (product_id,))
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
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = \'installer\' AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version = ? AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, installer_id, installer_os, installer_language, installer_version, installer_file_id, installer_file_size))
            else:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = \'installer\' AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version IS NULL AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, installer_id, installer_os, installer_language, installer_file_id, installer_file_size))
            
            entry_pk = db_cursor.fetchone()
            
            if entry_pk is None:
                # gf_int_nr, gf_int_added, gf_int_removed, gf_int_id, gf_int_download_type,
                # gf_id, gf_name, gf_os, gf_language, gf_version,
                # gf_type, gf_count, gf_total_size, gf_file_id, gf_file_size
                db_cursor.execute(INSERT_FILES_QUERY, (None, datetime.now(), None, product_id, 'installer', 
                                                       installer_id, installer_product_name, installer_os, installer_language, installer_version, 
                                                       None, None, installer_total_size, installer_file_id, installer_file_size))
                # no need to print the os here, as it's included in the installer_id
                logger.info(f'FQ +++ Added DB entry for {product_id}: {installer_product_name}, {installer_id}, {installer_version}.')
            
            else:
                logger.debug(f'FQ >>> Found an existing entry for {product_id}: {installer_product_name}, {installer_id}, {installer_version}.')
                listed_installer_pks.remove(entry_pk[0])
    
    if len(listed_installer_pks) > 0:
        for removed_pk in listed_installer_pks:
            db_cursor.execute('UPDATE gog_files SET gf_int_removed = ? WHERE gf_int_nr = ? AND gf_int_removed IS NULL', (datetime.now(), removed_pk))
        
        logger.info(f'FQ --- Marked some installer entries as removed for {product_id}')
    
    # process patch entries
    db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? '
                      'AND gf_int_download_type = \'patch\' AND gf_int_removed IS NULL', (product_id,))
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
        # replace blank patch version with None (blanks happens with patches, but not with installers)
        if patch_version == '': patch_version = None
        patch_total_size = patch_entry['total_size']
        
        for patch_file in patch_entry['files']:
            patch_file_id = patch_file['id']
            patch_file_size = patch_file['size']
            
            if patch_version is not None:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = \'patch\' AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version = ? AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, patch_id, patch_os, patch_language, patch_version, patch_file_id, patch_file_size))
            else:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = \'patch\' AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version IS NULL AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, patch_id, patch_os, patch_language, patch_file_id, patch_file_size))
            
            entry_pk = db_cursor.fetchone()
            
            if entry_pk is None:
                # gf_int_nr, gf_int_added, gf_int_removed, gf_int_id, gf_int_download_type, 
                # gf_id, gf_name, gf_os, gf_language, gf_version, 
                # gf_type, gf_count, gf_total_size, gf_file_id, gf_file_size
                db_cursor.execute(INSERT_FILES_QUERY, (None, datetime.now(), None, product_id, 'patch', 
                                                       patch_id, patch_product_name, patch_os, patch_language, patch_version, 
                                                       None, None, patch_total_size, patch_file_id, patch_file_size))
                # no need to print the os here, as it's included in the patch_id
                logger.info(f'FQ +++ Added DB entry for {product_id}: {patch_product_name}, {patch_id}, {patch_version}.')
            
            else:
                logger.debug(f'FQ >>> Found an existing entry for {product_id}: {patch_product_name}, {patch_id}, {patch_version}.')
                listed_patch_pks.remove(entry_pk[0])
    
    if len(listed_patch_pks) > 0:
        for removed_pk in listed_patch_pks:
            db_cursor.execute('UPDATE gog_files SET gf_int_removed = ? WHERE gf_int_nr = ? AND gf_int_removed IS NULL', (datetime.now(), removed_pk))
        
        logger.info(f'FQ --- Marked some patch entries as removed for {product_id}')
    
    # process language_packs entries
    db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? '
                      'AND gf_int_download_type = \'language_packs\' AND gf_int_removed IS NULL', (product_id,))
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
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = \'language_packs\' AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version = ? AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, language_pack_id, language_pack_os, language_pack_language, language_pack_version, 
                                   language_pack_file_id, language_pack_file_size))
            else:
                db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = \'language_packs\' AND gf_id = ? '
                                  'AND gf_os = ? AND gf_language = ? AND gf_version IS NULL AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                                  (product_id, language_pack_id, language_pack_os, language_pack_language, 
                                   language_pack_file_id, language_pack_file_size))
            
            entry_pk = db_cursor.fetchone()
            
            if entry_pk is None:
                # gf_int_nr, gf_int_added, gf_int_removed, gf_int_id, gf_int_download_type, gf_id, 
                # gf_name, gf_os, gf_language, gf_version, 
                # gf_type, gf_count, gf_total_size, gf_file_id, gf_file_size
                db_cursor.execute(INSERT_FILES_QUERY, (None, datetime.now(), None, product_id, 'language_packs', language_pack_id, 
                                                       language_pack_product_name, language_pack_os, language_pack_language, language_pack_version, 
                                                       None, None, language_pack_total_size, language_pack_file_id, language_pack_file_size))
                # no need to print the os here, as it's included in the patch_id
                logger.info(f'FQ +++ Added DB entry for {product_id}: {language_pack_product_name}, {language_pack_id}, {language_pack_version}.')
            
            else:
                logger.debug(f'FQ >>> Found an existing entry for {product_id}: {language_pack_product_name}, {language_pack_id}, {language_pack_version}.')
                listed_language_packs_pks.remove(entry_pk[0])
    
    if len(listed_language_packs_pks) > 0:
        for removed_pk in listed_language_packs_pks:
            db_cursor.execute('UPDATE gog_files SET gf_int_removed = ? WHERE gf_int_nr = ? AND gf_int_removed IS NULL', (datetime.now(), removed_pk))
        
        logger.info(f'FQ --- Marked some language_pack entries as removed for {product_id}')
    
    # process bonus_content entries
    db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? '
                      'AND gf_int_download_type = \'bonus_content\' AND gf_int_removed IS NULL', (product_id,))
    listed_bonus_content_pks = [pk_result[0] for pk_result in db_cursor.fetchall()]
    
    for bonus_content_entry in json_parsed_bonus_content:
        bonus_content_id = bonus_content_entry['id']
        bonus_content_product_name = bonus_content_entry['name'].strip()
        # bonus content type 'guides & reference ' has a trailing space
        bonus_content_type = bonus_content_entry['type'].strip()
        bonus_content_count = bonus_content_entry['count']
        bonus_content_total_size = bonus_content_entry['total_size']
        
        for bonus_content_file in bonus_content_entry['files']:
            bonus_content_file_id = bonus_content_file['id']
            bonus_content_file_size = bonus_content_file['size']
            
            db_cursor.execute('SELECT gf_int_nr FROM gog_files WHERE gf_int_id = ? AND gf_int_download_type = \'bonus_content\' AND gf_id = ? '
                              'AND gf_type = ? AND gf_count = ? AND gf_file_id = ? AND gf_file_size = ? AND gf_int_removed IS NULL', 
                              (product_id, bonus_content_id, bonus_content_type, bonus_content_count, bonus_content_file_id, bonus_content_file_size))
            
            entry_pk = db_cursor.fetchone()
            
            if entry_pk is None:
                # gf_int_nr, gf_int_added, gf_int_removed, gf_int_id, gf_int_download_type, 
                # gf_id, gf_name, gf_os, gf_language, gf_version, 
                # gf_type, gf_count, gf_total_size, gf_file_id, gf_file_size
                db_cursor.execute(INSERT_FILES_QUERY, (None, datetime.now(), None, product_id, 'bonus_content', 
                                                       bonus_content_id, bonus_content_product_name, None, None, None, 
                                                       bonus_content_type, bonus_content_count, bonus_content_total_size, 
                                                       bonus_content_file_id, bonus_content_file_size))
                # print the entry type, since bonus_content entries are not versioned
                logger.info(f'FQ +++ Added DB entry for {product_id}: {bonus_content_product_name}, {bonus_content_id}, {bonus_content_type}.')
            
            else:
                logger.debug(f'FQ >>> Found an existing entry for {product_id}: {bonus_content_product_name}, {bonus_content_id}, {bonus_content_type}.')
                listed_bonus_content_pks.remove(entry_pk[0])
    
    if len(listed_bonus_content_pks) > 0:
        for removed_pk in listed_bonus_content_pks:
            db_cursor.execute('UPDATE gog_files SET gf_int_removed = ? WHERE gf_int_nr = ? AND gf_int_removed IS NULL', (datetime.now(), removed_pk))
        
        logger.info(f'FQ --- Marked some bonus_content entries as removed for {product_id}')
    
    db_connection.commit()

def gog_products_bulk_query(process_tag, product_id, scan_mode, db_lock, session, db_connection):
    # generate a string of comma separated ids in the current batch
    product_ids_string = ','.join([str(product_id_value) for product_id_value in range(product_id, product_id + IDS_IN_BATCH)])
    logger.debug(f'{process_tag}BQ >>> Processing the following product_id string batch: {product_ids_string}.')
    
    bulk_products_url = f'https://api.gog.com/products?ids={product_ids_string}'
    
    try:
        response = session.get(bulk_products_url, timeout=HTTP_TIMEOUT)
        
        logger.debug(f'{process_tag}BQ >>> HTTP response code: {response.status_code}.')
        
        if response.status_code == HTTP_OK and response.text != '[]':
            logger.info(f'{process_tag}BQ >>> Found something in the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range...')
            
            json_parsed = json.loads(response.text, object_pairs_hook=OrderedDict)
            
            for line in json_parsed:
                current_product_id = line['id']
                retries_complete = False
                retry_counter = 0
                
                while not retries_complete:
                    if retry_counter > 0:
                        logger.warning(f'{process_tag}BQ >>> Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                        sleep(RETRY_SLEEP_INTERVAL)
                        logger.warning(f'{process_tag}BQ >>> Reprocessing id {current_product_id}...')
                    
                    retries_complete = gog_product_extended_query(process_tag, current_product_id, scan_mode, db_lock, 
                                                                  session, db_connection)
                    
                    if retries_complete:
                        if retry_counter > 1:
                            logger.info(f'{process_tag}BQ >>> Succesfully retried for {current_product_id}.')
                    else:
                        retry_counter += 1
        
        # this should not be handled as an exception, as it's the default behavior when nothing is detected
        elif response.status_code == HTTP_OK and response.text == '[]':
            logger.debug(f'{process_tag}BQ >>> A blank list entry ([]) received.')
        
        else:
            logger.warning(f'{process_tag}BQ >>> HTTP error code {response.status_code} received for the {product_id} '
                           f'<-> {product_id + IDS_IN_BATCH - 1} range.')
            raise Exception()
        
        return True
    
    # sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning(f'{process_tag}BQ >>> Connection SSL error encountered for the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range.')
        return False
    
    # sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning(f'{process_tag}BQ >>> Connection error encountered for the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range.')
        return False
    
    except:
        logger.debug(f'{process_tag}BQ >>> Products bulk query has failed for the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range.')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        
        return False

def worker_process(process_tag, scan_mode, id_queue, db_lock, config_lock, 
                   fail_event, terminate_event):
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)
    
    processConfigParser = ConfigParser()
    
    with requests.Session() as processSession, sqlite3.connect(DB_FILE_PATH) as process_db_connection:
        logger.info(f'{process_tag}>>> Starting worker process...')
        
        try:
            while not terminate_event.is_set():
                product_id = id_queue.get(True, QUEUE_WAIT_TIMEOUT)
                
                retry_counter = 0
                retries_complete = False
                
                while not retries_complete and not terminate_event.is_set():
                    if retry_counter > 0:
                        logger.debug(f'{process_tag}>>> Retry count: {retry_counter}.')
                        # main iteration incremental sleep
                        sleep((retry_counter ** RETRY_AMPLIFICATION_FACTOR) * RETRY_SLEEP_INTERVAL)
                        
                        retries_complete = gog_products_bulk_query(process_tag, product_id, scan_mode, db_lock, 
                                                                   processSession, process_db_connection)
                    
                    if retries_complete:
                        if retry_counter > 0:
                            logger.info(f'{process_tag}>>> Succesfully retried for the {product_id} <-> {product_id + IDS_IN_BATCH - 1} range.')
                    else:
                        retry_counter += 1
                        # terminate the scan if the RETRY_COUNT limit is exceeded
                        if retry_counter > RETRY_COUNT:
                            logger.critical(f'{process_tag}>>> Request most likely blocked/invalidated by GOG. Terminating process!')
                            fail_event.set()
                            terminate_event.set()
                
                if product_id % ID_SAVE_INTERVAL == 0 and not terminate_event.is_set():
                    with config_lock:
                        processConfigParser.read(CONF_FILE_PATH)
                        processConfigParser['FULL_SCAN']['start_id'] = str(product_id)
                        
                        with open(CONF_FILE_PATH, 'w') as file:
                            processConfigParser.write(file)
                        
                        logger.info(f'{process_tag}>>> Processed up to id: {product_id}...')
        
        # the main process has stopped populating the queue if this exception is raised
        except queue.Empty:
            logger.debug(f'{process_tag}>>> Timed out while waiting for queue.')
        
        except SystemExit:
            pass
        
        logger.info(f'{process_tag}>>> Stopping worker process...')
        
        logger.debug(f'{process_tag}>>> Running PRAGMA optimize...')
        with db_lock:
            process_db_connection.execute(OPTIMIZE_QUERY)

if __name__ == "__main__":
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)
    
    parser = argparse.ArgumentParser(description=('GOG products scan (part of gog_gles) - a script to call publicly available GOG APIs '
                                                  'in order to retrieve product information and updates.'))
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-f', '--full', help='Perform a full products scan using the Galaxy products endpoint', action='store_true')
    group.add_argument('-u', '--update', help='Run an update scan for existing products', action='store_true')
    group.add_argument('-n', '--new', help='Query new products', action='store_true')
    group.add_argument('-b', '--builds', help='Perform a product scan based on unknown builds', action='store_true')
    group.add_argument('-r', '--releases', help='Perform a product scan based on missing external releases', action='store_true')
    group.add_argument('-e', '--extract', help='Extract file data from existing products', action='store_true')
    group.add_argument('-m', '--manual', help='Perform a manual products scan', action='store_true')
    group.add_argument('-d', '--delisted', help='Perform a scan on all the delisted products', action='store_true')
    
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
        RETRY_AMPLIFICATION_FACTOR = general_section.getint('retry_amplification_factor')
        # ids that don't have a valid v2 endpoint for some reason
        NO_V2_ENDPOINT = [int(product_id.strip()) for product_id in 
                          general_section.get('no_v2_endpoint').split(',')]
    except:
        logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
        raise SystemExit(1)
    
    try:
        # read a static list of movie ids from a csv file and use it to determine
        # which entries should be treated as movies (movies have been more or less
        # abandoned by GOG, so it's doubtful these ids will change going forward)
        with open(MOVIES_ID_CSV_PATH, 'r') as file:
            MOVIES_ID_LIST = [int(movie_id) for movie_id in file.read().split()]
        
        logger.debug(f'Read the following movie ids: {MOVIES_ID_LIST}')
    except:
        logger.critical('Could not parse movie ids csv file!')
        raise SystemExit(2)
    
    logger.info('*** Running PRODUCTS scan script ***')
    
    # detect any parameter overrides and set the scan_mode accordingly
    if len(argv) > 1:
        logger.info('Command-line parameter mode override detected.')
        
        if args.full:
            scan_mode = 'full'
        elif args.update:
            scan_mode = 'update'
        elif args.new:
            scan_mode = 'new'
        elif args.builds:
            scan_mode = 'builds'
        elif args.releases:
            scan_mode = 'releases'
        elif args.extract:
            scan_mode = 'extract'
        elif args.manual:
            scan_mode = 'manual'
        elif args.delisted:
            scan_mode = 'delisted'
    
    # boolean 'true' or scan_mode specific activation
    if CONF_BACKUP == 'true' or CONF_BACKUP == scan_mode:
        if os.path.exists(CONF_FILE_PATH):
            # create a backup of the existing conf file - mostly for debugging/recovery
            copy2(CONF_FILE_PATH, CONF_FILE_PATH + '.bak')
            logger.info('Successfully created conf file backup.')
        else:
            logger.critical('Could find specified conf file!')
            raise SystemExit(3)
    
    # boolean 'true' or scan_mode specific activation
    if DB_BACKUP == 'true' or DB_BACKUP == scan_mode:
        if os.path.exists(DB_FILE_PATH):
            # create a backup of the existing db - mostly for debugging/recovery
            copy2(DB_FILE_PATH, DB_FILE_PATH + '.bak')
            logger.info('Successfully created db backup.')
        else:
            #subprocess.run(['python', 'gog_create_db.py'])
            logger.critical('Could find specified DB file!')
            raise SystemExit(4)
    
    # inter-process resources locks
    db_lock = multiprocessing.Lock()
    config_lock = multiprocessing.Lock()
    # shared process events
    terminate_event = multiprocessing.Event()
    terminate_event.clear()
    fail_event = multiprocessing.Event()
    fail_event.clear()
    
    if scan_mode == 'full':
        logger.info('--- Running in FULL scan mode ---')
        
        full_scan_section = configParser['FULL_SCAN']
        ID_SAVE_INTERVAL = full_scan_section.getint('id_save_interval')
        # 50 is the max id batch size allowed by the bulk products API
        IDS_IN_BATCH = full_scan_section.getint('ids_in_batch')
        # number of active connection processes
        CONNECTION_PROCESSES = full_scan_section.getint('connection_processes')
        # stop_id = 2147483647, in order to scan the full range,
        # stopping at the upper limit of a 32 bit signed integer type
        STOP_ID = full_scan_section.getint('stop_id')
        # product_id will restart from scan_id
        product_id = full_scan_section.getint('start_id')
        # reduce starting point by a batch to account for any process overlap
        if product_id > ID_SAVE_INTERVAL: product_id -= ID_SAVE_INTERVAL
        
        logger.info(f'Restarting scan from id: {product_id}.')
        
        stop_id_reached = False
        id_queue = multiprocessing.Queue(CONNECTION_PROCESSES * 2)
        process_list = []
        
        try:
            for process_no in range(CONNECTION_PROCESSES):
                # apply spacing to single digit process_no for nicer logging in case of 10+ processes
                PROCESS_LOGGING_FILLER = '0' if CONNECTION_PROCESSES > 9 and process_no < 9 else ''
                process_tag_nice = ''.join(('P#', PROCESS_LOGGING_FILLER, str(process_no + 1), ' '))
                
                process = multiprocessing.Process(target=worker_process,
                                                  args=(process_tag_nice, scan_mode, id_queue, db_lock, config_lock, 
                                                        fail_event, terminate_event), 
                                                  daemon=True)
                process.start()
                process_list.append(process)
                sleep(PROCESS_START_WAIT_INTERVAL)
            
            while not stop_id_reached and not terminate_event.is_set():
                try:
                    # pass only the start product_id for the current batch
                    id_queue.put(product_id, True, QUEUE_WAIT_TIMEOUT)
                    # skip an IDS_IN_BATCH interval
                    product_id += IDS_IN_BATCH
                    
                    if product_id > STOP_ID:
                        logger.info(f'Stop id of {STOP_ID} reached. Halting processing...')
                        stop_id_reached = True
                
                except queue.Full:
                    logger.debug('Timed out on queue insert.')
        
        except SystemExit:
            try:
                terminate_event.set()
                logger.info('Stopping full scan...')
            except:
                terminate_event.set()
        
        finally:
            logger.info('Waiting for the worker processes to complete...')
            
            for process in process_list:
                process.join()
            
            logger.info('The worker processes have been stopped.')
    
    elif scan_mode == 'update':
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
                db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id > ? '
                                                  'AND gp_int_delisted IS NULL ORDER BY 1', (last_id,))
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all existing product ids from the DB...')
                
                last_id_counter = 0
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')
                        
                        retries_complete = gog_product_extended_query('', current_product_id, scan_mode, db_lock, 
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
                                fail_event.set()
                                terminate_event.set()
                    
                    if last_id_counter % ID_SAVE_FREQUENCY == 0 and not terminate_event.is_set():
                        configParser.read(CONF_FILE_PATH)
                        configParser['UPDATE_SCAN']['last_id'] = str(current_product_id)
                        
                        with open(CONF_FILE_PATH, 'w') as file:
                            configParser.write(file)
                        
                        logger.info(f'Saved scan up to last_id of {current_product_id}.')
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping update scan...')
    
    elif scan_mode == 'new':
        logger.info('--- Running in NEW scan mode ---')
        
        try:
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                logger.info('Running scan for new arrival entries...')
                page_no = 1
                # start off with 1, then use whatever is returned by the API call
                new_page_count = 1
                # use default website pagination, which means the response can be split across 2+ pages in the API call
                while page_no <= new_page_count and not terminate_event.is_set():
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing new arrivals page {page_no}...')
                        
                        new_params = ''.join(('limit=48&releaseStatuses=in:new-arrival&order=desc:releaseDate&productType=in:game,pack,dlc,extras&page=', 
                                              # locales and currency don't matter here, but emulate default GOG website behavior
                                              str(page_no), '&countryCode=BE&locale=en-US&currencyCode=EUR'))
                        retries_complete, new_page_count = gog_product_games_catalog_query(new_params, scan_mode, db_lock, 
                                                                                           session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for page {page_no}.')
                            
                            page_no += 1
                        
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()
                
                logger.info('Running scan for upcoming entries...')
                page_no = 1
                # start off with 1, then use whatever is returned by the API call
                upcoming_page_count = 1
                # use default website pagination, which means the response can be split across 2+ pages in the API call
                while page_no <= upcoming_page_count and not terminate_event.is_set():
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing upcoming entries page {page_no}...')
                        
                        upcoming_params = ''.join(('limit=48&releaseStatuses=in:upcoming&order=desc:releaseDate&productType=in:game,pack,dlc,extras&page=', 
                                                   # locales and currency don't matter here, but emulate default GOG website behavior
                                                   str(page_no), '&countryCode=BE&locale=en-US&currencyCode=EUR'))
                        retries_complete, upcoming_page_count = gog_product_games_catalog_query(upcoming_params, scan_mode, db_lock, 
                                                                                                session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for page {page_no}.')
                            
                            page_no += 1
                        
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping new scan...')
    
    elif scan_mode == 'builds':
        logger.info('--- Running in BUILDS scan mode ---')
        
        try:
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                db_cursor = db_connection.execute('SELECT gb_int_id FROM gog_builds WHERE gb_int_title IS NULL ORDER BY 1')
                id_list = db_cursor.fetchall()
                
                logger.debug('Retrieved all unidentified build product ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')
                        
                        retries_complete = gog_product_extended_query('', current_product_id, scan_mode, db_lock, 
                                                                      session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping builds scan...')
    
    elif scan_mode == 'releases':
        logger.info('--- Running in RELEASES scan mode ---')
        
        try:
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                db_cursor = db_connection.execute('SELECT gr_external_id FROM gog_releases WHERE gr_external_id NOT IN '
                                                  '(SELECT gp_id FROM gog_products ORDER BY 1) ORDER BY 1')
                id_list = db_cursor.fetchall()
                
                logger.debug('Retrieved all missing external releases ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')
                        
                        retries_complete = gog_product_extended_query('', current_product_id, scan_mode, db_lock, 
                                                                      session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping releases scan...')
    
    elif scan_mode == 'extract':
        logger.info('--- Running in FILE EXTRACT scan mode ---')
        
        try:
            with sqlite3.connect(DB_FILE_PATH) as db_connection:
                db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_int_delisted IS NULL ORDER BY 1')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all existing product ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    
                    gog_files_extract_parser(db_connection, current_product_id)
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping extract scan...')
            
    elif scan_mode == 'manual':
        logger.info('--- Running in MANUAL scan mode ---')
        
        manual_scan_section = configParser['MANUAL_SCAN']
        id_list = manual_scan_section.get('id_list')
        
        if id_list == '':
            logger.warning('Nothing to scan!')
            raise SystemExit(0)
        
        try:
            id_list = [int(product_id.strip()) for product_id in id_list.split(',')]
        except ValueError:
            logger.critical('Could not parse id list!')
            raise SystemExit(5)
        
        try:
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                for product_id in id_list:
                    logger.info(f'Running scan for id {product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {product_id}...')
                        
                        retries_complete = gog_product_extended_query('', product_id, scan_mode, db_lock, 
                                                                      session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {product_id}.')
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping manual scan...')
    
    elif scan_mode == 'delisted':
        logger.info('--- Running in DELISTED scan mode ---')
        
        try:
            with requests.Session() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
                db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_int_delisted IS NOT NULL ORDER BY 1')
                id_list = db_cursor.fetchall()
                logger.debug('Retrieved all delisted product ids from the DB...')
                
                for id_entry in id_list:
                    current_product_id = id_entry[0]
                    logger.debug(f'Now processing id {current_product_id}...')
                    retries_complete = False
                    retry_counter = 0
                    
                    while not retries_complete and not terminate_event.is_set():
                        if retry_counter > 0:
                            logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                            sleep(RETRY_SLEEP_INTERVAL)
                            logger.warning(f'Reprocessing id {current_product_id}...')
                        
                        retries_complete = gog_product_extended_query('', current_product_id, scan_mode, db_lock, 
                                                                      session, db_connection)
                        
                        if retries_complete:
                            if retry_counter > 0:
                                logger.info(f'Succesfully retried for {current_product_id}.')
                        else:
                            retry_counter += 1
                            # terminate the scan if the RETRY_COUNT limit is exceeded
                            if retry_counter > RETRY_COUNT:
                                logger.critical('Retry count exceeded, terminating scan!')
                                fail_event.set()
                                terminate_event.set()
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            terminate_event.set()
            logger.info('Stopping delisted scan...')
    
    if not terminate_event.is_set() and scan_mode == 'update':
        logger.info('Resetting last_id parameter...')
        configParser.read(CONF_FILE_PATH)
        configParser['UPDATE_SCAN']['last_id'] = ''
        
        with open(CONF_FILE_PATH, 'w') as file:
            configParser.write(file)
    
    logger.info('All done! Exiting...')
    
    # return a non-zero exit code if a scan failure was encountered
    if terminate_event.is_set() and fail_event.is_set():
        raise SystemExit(6)
