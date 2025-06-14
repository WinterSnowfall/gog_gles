#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 5.00
@date: 14/06/2025

Warning: Built for use with python 3.6+
'''

import sqlite3
import signal
import requests
# GOG support links are protected by Cloudflare's anti-bot page
import cloudscraper
import logging
import argparse
import os
from shutil import copy2
from configparser import ConfigParser
from datetime import datetime
from time import sleep
from lxml import html as lhtml
from logging.handlers import RotatingFileHandler
# uncomment for debugging purposes only
#import traceback

from common.gog_constants_interface import ConstantsInterface

# attempt to import an HTTPS proxy interface implementation
try:
    from common.gog_proxy_interface import ProxyInterface
    PROXY_INTERFACE_IS_IMPORTED = True
except ImportError:
    PROXY_INTERFACE_IS_IMPORTED = False

# conf file block
CONF_FILE_PATH = os.path.join('..', 'conf', 'gog_support_scan.conf')

# logging configuration block
LOG_FILE_PATH = os.path.join('..', 'logs', 'gog_support_scan.log')
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
INSERT_SUPPORT_QUERY = 'INSERT INTO gog_support VALUES (?,?,?,?,?)'

def sigterm_handler(signum, frame):
    logger.debug('Stopping scan due to SIGTERM...')

    raise SystemExit(0)

def sigint_handler(signum, frame):
    logger.debug('Stopping scan due to SIGINT...')

    raise SystemExit(0)

def start_proxy_process(proxy_binary_path, proxy_startup_delay):
    # use Popen since we don't want to wait for the process to complete
    subprocess.Popen(proxy_binary_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # wait for the proxy startup process to complete
    sleep(proxy_startup_delay)

    # run a check to confirm that the proxy process has started properly
    try:
        # no need to check the output, only to ensure a non-zero exit code is returned
        subprocess.run(['pgrep', PROXY_PROCESS_NAMES[0]],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    except subprocess.CalledProcessError:
        logger.critical('The proxy process has failed to start. Terminating script!')
        # use a '0' return code in order to prevent container services from treating this as an error
        raise SystemExit(0)

def stop_proxy_process():
    # use pkill to terminate all the proxy processes based on the supplied process name list
    for proxy_process_name in PROXY_PROCESS_NAMES:
        try:
            subprocess.run(['pkill', proxy_process_name],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            # wait for the proxy process to terminate
            sleep(1)
        except subprocess.CalledProcessError:
            # warn only on the first process name, since it should ensure the termination of all other proxy processes
            if proxy_process_name == PROXY_PROCESS_NAMES[0]:
                logger.warning(f'No running proxy process \'{proxy_process_name}\' detected on attempted stop.')

def gog_support_query(https_proxy, session, db_connection, support_url, support_links):

    logger.debug(f'SQ >>> Querying url: {support_url}.')

    try:
        # use an HTTPS proxy if configured to do so
        if https_proxy:
            response = session.get(support_url, headers=ConstantsInterface.HEADERS, cookies=ConstantsInterface.COOKIES,
                                   proxies=ProxyInterface.PROXIES, timeout=HTTP_TIMEOUT)
            # NOTE: The HTTPS proxy will not automatically refresh the IP if the connection is throttled,
            # however its use will allow the script to run during a temporary IP ban
        else:
            response = session.get(support_url, headers=ConstantsInterface.HEADERS, cookies=ConstantsInterface.COOKIES,
                                   timeout=HTTP_TIMEOUT)

        logger.debug(f'SQ >>> HTTP response code: {response.status_code}.')

        if response.status_code == ConstantsInterface.HTTP_OK:
            html_tree = lhtml.fromstring(response.text)

            end_page = False if len(html_tree.xpath('//p/i/a[contains(@href, '
                                                    '"/hc/en-us/sections/203122865-All-games")]/text()')) == 0 else True
            logger.debug(f'SQ >>> end_page is: {end_page}.')

            if not end_page:
                link_elements = html_tree.xpath('//ul[contains(@class, "article-list")]'
                                                '/li[contains(@class, "article-list-item ")]'
                                                '/a[contains(@class, "article-list-item__link article-list-item__link_big")]')

                for link_element in link_elements:
                    support_link = 'https://www.gog.com' + link_element.xpath('@href')[0]
                    support_links.append(f'"{support_link}"')

                    support_name = link_element.xpath('text()')[0].strip()
                    # some support page names append 'The' rather than prepending it, which is what the APIs do...
                    # and not all of these actually end with ', The', for example: 'Ballads of Reemus, The: When The Bed Bites',
                    # so do a search and replace to cater for all possible stupidity and nonsense (one can only hope)
                    if support_name.find(', The') != -1:
                        logger.debug('SQ >>> Correcting article position...')
                        support_name = ''.join(('The ', support_name.replace(', The', '')))
                    # Colonization, Sid Meier's... FFS...
                    if support_name.endswith(', Sid Meier\'s'):
                        logger.debug('SQ >>> Properly praising Sid Meier...')
                        support_name = ''.join(('Sid Meier\'s ', support_name[:-13]))

                    logger.debug(f'SQ >>> Parsed entry with support link: {support_link}, support name: {support_name}')

                    db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_support WHERE gs_link = ?', (support_link,))
                    entry_count = db_cursor.fetchone()[0]

                    if entry_count == 0:
                        # gfr_int_nr, gfr_int_added, gfr_int_removed, gfr_name, gfr_link
                        db_cursor.execute(INSERT_SUPPORT_QUERY, (None, datetime.now().isoformat(' '), None, support_name, support_link))
                        db_connection.commit()
                        logger.info(f'SQ +++ Added a new DB entry for {support_name}.')

                    elif entry_count == 1:
                        db_cursor.execute('SELECT gs_int_removed, gs_name FROM gog_support WHERE gs_link = ?', (support_link,))
                        existing_removed, existing_name = db_cursor.fetchone()

                        # clear the removed status if a support link is readded (should only happen rarely)
                        if existing_removed is not None:
                            logger.debug(f'SQ >>> Found a previously removed entry with name {support_name}. Clearing removed status...')
                            db_cursor.execute('UPDATE gog_support SET gs_int_removed = NULL WHERE gs_link = ?', (support_link,))
                            db_connection.commit()
                            logger.info(f'SQ *** Cleared removed status for {support_name}.')

                        # this should be very unlikely, yet properly update the entry name if it gets changed for some reason
                        if existing_name != support_name:
                            logger.debug(f'SQ >>> Existing entry for {support_name} is outdated. Updating...')
                            db_cursor.execute('UPDATE gog_support SET gs_name = ? WHERE gs_link = ?', (support_name, support_link))
                            db_connection.commit()
                            logger.info(f'SQ ~~~ Updated the DB entry for {support_name}.')

            else:
                logger.debug('SQ >>> Support page parsing complete.')

                # general pass to mark undetected but existing entries as removed
                exclusion_list = ', '.join(support_links)

                db_cursor = db_connection.execute('SELECT COUNT(*) FROM gog_support WHERE gs_int_removed IS NULL '
                                                  f'AND gs_link NOT IN ({exclusion_list})')
                entry_count = db_cursor.fetchone()[0]

                if entry_count == 0:
                    logger.debug('SQ >>> No entries to mark as removed. Skipping.')
                else:
                    db_cursor.execute('SELECT gs_link FROM gog_support WHERE gs_int_removed IS NULL '
                                     f'AND gs_link NOT IN ({exclusion_list})')
                    support_link_list = [support_link[0] for support_link in db_cursor.fetchall()]

                    for support_link in support_link_list:
                        logger.debug(f'SQ >>> Support page {support_link} has been removed...')
                        db_cursor.execute('UPDATE gog_support SET gs_int_removed = ? WHERE gs_link = ?',
                                          (datetime.now().isoformat(' '), support_link))
                        db_connection.commit()
                        logger.warning(f'SQ --- Marked the DB entry for {support_link} as removed.')

            return (True, end_page, support_links)

        else:
            logger.warning(f'SQ >>> HTTP error code {response.status_code} received.')
            return (False, False, support_links)

    # sometimes the connection may time out
    except requests.Timeout:
        logger.warning(f'SQ >>> HTTP request timed out after {HTTP_TIMEOUT} seconds.')
        return (False, False, support_links)

    # sometimes the HTTPS connection encounters SSL errors
    except requests.exceptions.SSLError:
        logger.warning('SQ >>> Connection SSL error encountered.')
        return (False, False, support_links)

    # sometimes the HTTPS connection gets rejected/terminated
    except requests.exceptions.ConnectionError:
        logger.warning('SQ >>> Connection error encountered.')
        return (False, False, support_links)

    except:
        logger.debug('SQ >>> Support page query has failed.')
        # uncomment for debugging purposes only
        #logger.error(traceback.format_exc())
        return (False, False, support_links)

if __name__ == "__main__":
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)

    parser = argparse.ArgumentParser(description=('GOG support scan (part of gog_gles) - a script to scrape the GOG website '
                                                  'in order to retrieve existing support pages for game entries.'))

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

        DB_BACKUP = general_section.get('db_backup')
        HTTP_TIMEOUT = general_section.getint('http_timeout')
        RETRY_COUNT = general_section.getint('retry_count')
        RETRY_SLEEP_INTERVAL = general_section.getint('retry_sleep_interval')

        # parsing proxy parameters
        proxy_section = configParser['PROXY']
        HTTPS_PROXY = PROXY_INTERFACE_IS_IMPORTED and proxy_section.getboolean('https_proxy')
        START_PROXY = proxy_section.getboolean('start_proxy')
        # these paths can be relative to the user's home folder
        PROXY_BINARY_PATH = os.path.expanduser(proxy_section.get('proxy_binary_path'))
        PROXY_CONF_PATH = os.path.expanduser(proxy_section.get('proxy_conf_path'))
        # parsing constants
        PROXY_STARTUP_DELAY = proxy_section.getint('proxy_startup_delay')
    except:
        logger.critical('Could not parse configuration file. Please make sure the appropriate structure is in place!')
        raise SystemExit(1)

    logger.info('*** Running SUPPORT scan script ***')

    # boolean 'true' or scan_mode specific activation
    if DB_BACKUP == 'true':
        if os.path.exists(DB_FILE_PATH):
            # create a backup of the existing db - mostly for debugging/recovery
            copy2(DB_FILE_PATH, DB_FILE_PATH + '.bak')
            logger.info('Successfully created db backup.')
        else:
            logger.critical('Could find specified DB file!')
            raise SystemExit(2)

    # for HTTPS proxy use
    if HTTPS_PROXY:
        logger.warning('+++ HTTPS proxy mode enabled +++')

        # set up the proxy interface
        ProxyInterface.logger = logger
        ProxyInterface.proxy_binary_path = PROXY_BINARY_PATH
        ProxyInterface.proxy_conf_path = PROXY_CONF_PATH
        ProxyInterface.proxy_startup_delay = PROXY_STARTUP_DELAY

        if START_PROXY:
            logger.info('Starting HTTPS proxy process...')
            # optionally also stop any existing proxy instances
            #ProxyInterface.stop_proxy_process()
            ProxyInterface.start_proxy_process()

    terminate_signal = False
    fail_signal = False

    logger.info('--- Running in FULL scan mode ---')

    try:
        with cloudscraper.create_scraper() as session, sqlite3.connect(DB_FILE_PATH) as db_connection:
            support_links = []
            end_page = False
            page_no = 1

            while not end_page and not terminate_signal:
                retries_complete = False
                retry_counter = 0

                while not retries_complete and not terminate_signal:
                    if retry_counter > 0:
                        logger.warning(f'Retry number {retry_counter}. Sleeping for {RETRY_SLEEP_INTERVAL}s...')
                        sleep(RETRY_SLEEP_INTERVAL)
                        logger.warning(f'Reprocessing support page {page_no}...')

                    support_url = f'https://support.gog.com/hc/en-us/sections/203122865-All-games?page={page_no}&product=gog'
                    retries_complete, end_page, support_links = gog_support_query(HTTPS_PROXY, session, db_connection,
                                                                                  support_url, support_links)

                    if retries_complete:
                        if retry_counter > 0:
                            logger.info(f'Succesfully retried for page {page_no}.')

                        page_no += 1

                    else:
                        retry_counter += 1
                        # terminate the scan if the RETRY_COUNT limit is exceeded
                        if retry_counter > RETRY_COUNT:
                            logger.critical('Retry count exceeded, terminating scan!')
                            fail_signal = True
                            terminate_signal = True

            db_connection.execute(ConstantsInterface.OPTIMIZE_QUERY)

    except SystemExit:
        terminate_signal = True
        logger.info('Stopping support scan...')

    if HTTPS_PROXY and START_PROXY:
        ProxyInterface.stop_proxy_process()

    logger.info('All done! Exiting...')

    # return a non-zero exit code if a scan failure was encountered
    if terminate_signal and fail_signal:
        raise SystemExit(3)
