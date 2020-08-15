#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 1.50
@date: 10/08/2020

Warning: Built for use with python 3.6+
'''

import sqlite3
import logging
import argparse
from logging import FileHandler
from os import path

##logging configuration block
log_file_full_path = path.join('..', 'logs', 'gog_create_db.log')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler = FileHandler(log_file_full_path, mode='w', encoding='utf-8')
logger_file_formatter = logging.Formatter(logger_format)
logger_file_handler.setFormatter(logger_file_formatter)
logging.basicConfig(format=logger_format, level=logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
logger.addHandler(logger_file_handler)

##db configuration block
db_file_full_path = path.join('..', 'output_db', 'gog_visor.db')

##CONSTANTS
CREATE_GOG_COMPANIES_QUERY = ('CREATE TABLE gog_companies(gc_int_nr INTEGER PRIMARY KEY AUTOINCREMENT, '
                                'gc_int_added TEXT NOT NULL, '
                                'gc_int_no_longer_listed TEXT, '
                                'gc_name TEXT UNIQUE NOT NULL)')

CREATE_GOG_PRODUCTS_QUERY = ('CREATE TABLE gog_products(gp_int_nr INTEGER PRIMARY KEY AUTOINCREMENT, '
                                'gp_int_added TEXT NOT NULL, '
                                'gp_int_previous_update TEXT, '
                                'gp_int_no_longer_listed TEXT, '
                                'gp_int_dev_pub_null TEXT NOT NULL, '
                                'gp_int_latest_update TEXT, '
                                'gp_int_is_movie TEXT NOT NULL, '
                                'gp_int_product_url TEXT NOT NULL, '
                                'gp_int_previous_full_json_payload TEXT, '
                                'gp_int_full_json_payload TEXT NOT NULL, '
                                'gp_id INTEGER UNIQUE NOT NULL, '
                                'gp_title TEXT, '
                                'gp_slug TEXT NOT NULL, '
                                'gp_developer TEXT, '
                                'gp_publisher TEXT, '
                                'gp_developer_fk INTEGER, '
                                'gp_publisher_fk INTEGER, '
                                'gp_cs_compat_windows TEXT NOT NULL, '
                                'gp_cs_compat_osx TEXT NOT NULL, '
                                'gp_cs_compat_linux TEXT NOT NULL, '
                                'gp_languages TEXT NOT NULL, '
                                'gp_links_forum TEXT, '
                                'gp_links_product_card TEXT, '
                                'gp_links_purchase_link TEXT, '
                                'gp_links_support TEXT, '
                                'gp_in_development_active TEXT NOT NULL, '
                                'gp_in_development_until TEXT, '
                                'gp_is_secret TEXT NOT NULL, '
                                'gp_is_installable TEXT NOT NULL, '
                                'gp_game_type TEXT NOT NULL, '
                                'gp_is_pre_order TEXT NOT NULL, '
                                'gp_release_date TEXT, '
                                'gp_description_lead TEXT, '
                                'gp_description_full TEXT, '
                                'gp_description_cool TEXT, '
                                'gp_changelog TEXT)') #,
                                #disable foreign keys, since SQLite support for them is still sketchy
                                #'FOREIGN KEY(gp_author_fk) REFERENCES gog_companies(gc_int_nr)'
                                #'FOREIGN KEY(gp_publisher_fk) REFERENCES gog_companies(gc_int_nr))'

CREATE_GOG_FILES_QUERY = ('CREATE TABLE gog_files(gf_int_nr INTEGER PRIMARY KEY AUTOINCREMENT, '
                            'gf_int_added TEXT NOT NULL, '
                            'gf_int_product_id INTEGER NOT NULL, '
                            'gf_int_product_name TEXT, '
                            'gf_int_type TEXT NOT NULL, '
                            'gf_id INTEGER NOT NULL, '
                            'gf_name TEXT NOT NULL, '
                            'gf_os TEXT NOT NULL, '
                            'gf_language TEXT NOT NULL, '
                            'gf_version TEXT, '
                            'gf_total_size INTEGER NOT NULL, '
                            'gf_file_id TEXT NOT NULL, '
                            'gf_file_size INTEGER NOT NULL, '
                            'gf_file_downlink TEXT NOT NULL)')

##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description='GOG DB create (part of gog_visor) - a script to create the sqlite DB structure \
                                              for the other gog_visor utilities.')

args = parser.parse_args()

#db file check/creation section
if not path.exists(db_file_full_path):
    logger.info('No DB file detected. Creating new SQLite DB...')
    
    with sqlite3.connect(db_file_full_path) as db_connection:
        db_cursor = db_connection.cursor()
        db_cursor.execute(CREATE_GOG_COMPANIES_QUERY)
        db_cursor.execute(CREATE_GOG_PRODUCTS_QUERY)
        db_cursor.execute(CREATE_GOG_FILES_QUERY)
        db_cursor.execute('CREATE INDEX gf_int_product_id_index ON gog_files (gf_int_product_id)')
        db_connection.commit()
    
    logger.info('DB created successfully!')
else:
    logger.error('Existing DB file detected. Please delete the existing file if you are attempting to recreate the DB!')

##main thread end