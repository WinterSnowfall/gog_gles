#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 5.00
@date: 14/06/2025

Warning: Built for use with python 3.6+
'''

import sqlite3
import logging
import argparse
import os
from sys import argv

# logging configuration block
LOGGER_FORMAT = '%(asctime)s %(levelname)s >>> %(message)s'
# logging level for other modules
logging.basicConfig(format=LOGGER_FORMAT, level=logging.ERROR)
logger = logging.getLogger(__name__)
# logging level for current logger
logger.setLevel(logging.INFO) # DEBUG, INFO, WARNING, ERROR, CRITICAL

# db configuration block
DB_FILE_PATH = os.path.join('..', 'output_db', 'gog_gles.db')

# CONSTANTS
CREATE_GOG_BUILDS_QUERY = ('CREATE TABLE gog_builds (gb_int_nr INTEGER PRIMARY KEY, '
                           'gb_int_added TEXT NOT NULL, '
                           'gb_int_removed TEXT, '
                           'gb_int_updated TEXT, '
                           'gb_int_json_payload TEXT NOT NULL, '
                           'gb_int_json_diff TEXT, '
                           'gb_int_id INTEGER NOT NULL, '
                           'gb_int_title TEXT, '
                           'gb_int_os TEXT NOT NULL, '
                           'gb_total_count INTEGER NOT NULL, '
                           'gb_count INTEGER NOT NULL, '
                           'gb_main_version_names TEXT, '
                           'gb_branch_version_names TEXT, '
                           'gb_has_private_branches INTEGER NOT NULL)')

CREATE_GOG_FILES_QUERY = ('CREATE TABLE gog_files (gf_int_nr INTEGER PRIMARY KEY, '
                          'gf_int_added TEXT NOT NULL, '
                          'gf_int_removed TEXT, '
                          'gf_int_id INTEGER NOT NULL, '
                          'gf_int_download_type TEXT NOT NULL, '
                          'gf_id INTEGER NOT NULL, '
                          'gf_name TEXT NOT NULL, '
                          'gf_os TEXT, '
                          'gf_language TEXT, '
                          'gf_version TEXT, '
                          'gf_type TEXT, '
                          'gf_count INTEGER, '
                          'gf_total_size INTEGER NOT NULL, '
                          'gf_file_id TEXT NOT NULL, '
                          'gf_file_size INTEGER NOT NULL)')

CREATE_GOG_FORUMS_QUERY = ('CREATE TABLE gog_forums (gfr_int_nr INTEGER PRIMARY KEY, '
                          'gfr_int_added TEXT NOT NULL, '
                          'gfr_int_removed TEXT, '
                          'gfr_name TEXT UNIQUE NOT NULL, '
                          'gfr_link TEXT NOT NULL)')

CREATE_GOG_INSTALLERS_DELTA_QUERY = ('CREATE TABLE gog_installers_delta (gid_int_nr INTEGER PRIMARY KEY, '
                                     'gid_int_added TEXT NOT NULL, '
                                     'gid_int_fixed TEXT, '
                                     'gid_int_updated TEXT, '
                                     'gid_int_id INTEGER NOT NULL, '
                                     'gid_int_title TEXT NOT NULL, '
                                     'gid_int_os TEXT NOT NULL, '
                                     'gid_int_latest_galaxy_build TEXT NOT NULL, '
                                     'gid_int_latest_installer_version TEXT NOT NULL, '
                                     'gid_int_false_positive INTEGER NOT NULL, '
                                     'gid_int_false_positive_reason TEXT)')

CREATE_GOG_PRICES_QUERY = ('CREATE TABLE gog_prices (gpr_int_nr INTEGER PRIMARY KEY, '
                           'gpr_int_added TEXT NOT NULL, '
                           'gpr_int_outdated TEXT, '
                           'gpr_int_id INTEGER NOT NULL, '
                           'gpr_int_title TEXT NOT NULL, '
                           'gpr_int_country_code TEXT NOT NULL, '
                           'gpr_currency TEXT NOT NULL, '
                           'gpr_base_price REAL NOT NULL, '
                           'gpr_final_price REAL NOT NULL)')

CREATE_GOG_PRODUCTS_QUERY = ('CREATE TABLE gog_products (gp_int_nr INTEGER PRIMARY KEY, '
                             'gp_int_added TEXT NOT NULL, '
                             'gp_int_delisted TEXT, '
                             'gp_int_updated TEXT, '
                             'gp_int_json_payload TEXT NOT NULL, '
                             'gp_int_json_diff TEXT, '
                             'gp_int_v2_updated TEXT, '
                             'gp_int_v2_json_payload TEXT, '
                             'gp_int_v2_json_diff TEXT, '
                             'gp_id INTEGER UNIQUE NOT NULL, '
                             'gp_v2_title TEXT, '
                             'gp_v2_product_type TEXT, '
                             'gp_v2_developer TEXT, '
                             'gp_v2_publisher TEXT, '
                             'gp_v2_size INTEGER NOT NULL, '
                             'gp_v2_is_preorder INTEGER NOT NULL, '
                             'gp_v2_in_development INTEGER NOT NULL, '
                             'gp_v2_is_installable INTEGER NOT NULL, '
                             'gp_v2_os_support_windows INTEGER NOT NULL, '
                             'gp_v2_os_support_linux INTEGER NOT NULL, '
                             'gp_v2_os_support_osx INTEGER NOT NULL, '
                             'gp_v2_supported_os_versions TEXT, '
                             'gp_v2_global_release_date TEXT, '
                             'gp_v2_gog_release_date TEXT, '
                             'gp_v2_tags TEXT, '
                             'gp_v2_properties TEXT, '
                             'gp_v2_series TEXT, '
                             'gp_v2_features TEXT, '
                             'gp_v2_is_using_dosbox INTEGER NOT NULL, '
                             'gp_v2_links_store TEXT, '
                             'gp_v2_links_support TEXT, '
                             'gp_v2_links_forum TEXT, '
                             'gp_v2_description TEXT, '
                             'gp_v2_localizations TEXT, '
                             'gp_changelog TEXT)')

CREATE_GOG_RATINGS_QUERY = ('CREATE TABLE gog_ratings (grt_int_nr INTEGER PRIMARY KEY, '
                            'grt_int_added TEXT NOT NULL, '
                            'grt_int_removed TEXT, '
                            'grt_int_updated TEXT, '
                            'grt_int_json_payload TEXT NOT NULL, '
                            'grt_int_json_diff TEXT, '
                            'grt_int_id INTEGER UNIQUE NOT NULL, '
                            'grt_int_title TEXT NOT NULL, '
                            'grt_review_count INTEGER NOT NULL, '
                            'grt_avg_rating REAL NOT NULL, '
                            'grt_avg_rating_count INTEGER NOT NULL, '
                            'grt_avg_rating_verified_owner REAL NOT NULL, '
                            'grt_avg_rating_verified_owner_count INTEGER NOT NULL, '
                            'grt_is_reviewable INTEGER NOT NULL)')

CREATE_GOG_RELEASES_QUERY = ('CREATE TABLE gog_releases (gr_int_nr INTEGER PRIMARY KEY, '
                             'gr_int_added TEXT NOT NULL, '
                             'gr_int_delisted TEXT, '
                             'gr_int_updated TEXT, '
                             'gr_int_json_payload TEXT NOT NULL, '
                             'gr_int_json_diff TEXT, '
                             'gr_external_id INTEGER UNIQUE NOT NULL, '
                             'gr_title TEXT NOT NULL, '
                             'gr_type TEXT NOT NULL, '
                             'gr_supported_oses TEXT, '
                             'gr_genres TEXT, '
                             'gr_series TEXT, '
                             'gr_first_release_date TEXT, '
                             'gr_visible_in_library INTEGER NOT NULL, '
                             'gr_aggregated_rating REAL)')

CREATE_GOG_SUPPORT_QUERY = ('CREATE TABLE gog_support (gs_int_nr INTEGER PRIMARY KEY, '
                            'gs_int_added TEXT NOT NULL, '
                            'gs_int_removed TEXT, '
                            'gs_name TEXT NOT NULL, '
                            'gs_link TEXT UNIQUE NOT NULL)')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=('GOG DB schema (part of gog_gles) - a script to create the sqlite DB structure '
                                                  'for the other gog_gles utilities and maintain it.'))

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-c', '--create', help='Create the GOG DB and schema', action='store_true')
    group.add_argument('-v', '--vacuum', help='Vacuum (compact) the GOG DB', action='store_true')

    args = parser.parse_args()

    # set default operation mode
    db_mode = 'create';

    # detect any parameter overrides and set the db_mode accordingly
    if len(argv) > 1:
        logger.info('Command-line parameter mode override detected.')

        if args.create:
            db_mode = 'create'
        elif args.vacuum:
            db_mode = 'vacuum'

    if db_mode == 'create':
        logger.info('--- Running in CREATE DB mode ---')

        if not os.path.exists(DB_FILE_PATH):
            logger.info('No DB file detected. Creating new SQLite DB...')

            with sqlite3.connect(DB_FILE_PATH) as db_connection:
                db_cursor = db_connection.cursor()
                db_cursor.execute(CREATE_GOG_BUILDS_QUERY)
                db_cursor.execute('CREATE UNIQUE INDEX gb_int_id_os_index ON gog_builds (gb_int_id, gb_int_os)')
                db_cursor.execute(CREATE_GOG_FILES_QUERY)
                db_cursor.execute('CREATE INDEX gf_int_id_index ON gog_files (gf_int_id)')
                db_cursor.execute(CREATE_GOG_FORUMS_QUERY)
                db_cursor.execute(CREATE_GOG_INSTALLERS_DELTA_QUERY)
                db_cursor.execute('CREATE INDEX gid_int_id_os_index ON gog_installers_delta (gid_int_id, gid_int_os)')
                db_cursor.execute(CREATE_GOG_PRICES_QUERY)
                db_cursor.execute('CREATE INDEX gpr_int_id_index ON gog_prices (gpr_int_id)')
                db_cursor.execute(CREATE_GOG_PRODUCTS_QUERY)
                db_cursor.execute(CREATE_GOG_RATINGS_QUERY)
                db_cursor.execute(CREATE_GOG_RELEASES_QUERY)
                db_cursor.execute(CREATE_GOG_SUPPORT_QUERY)
                db_connection.commit()

            logger.info('DB created successfully.')
        else:
            logger.error('Existing DB file detected. Please delete the existing file if you are attempting to recreate the DB!')

    elif db_mode == 'vacuum':
        logger.info('--- Running in VACUUM DB mode ---')

        if os.path.exists(DB_FILE_PATH):
            logger.info('DB file detected. Vacuuming the DB...')

            with sqlite3.connect(DB_FILE_PATH) as db_connection:
                db_cursor = db_connection.cursor()
                db_cursor.execute('VACUUM')
                db_connection.commit()

            logger.info('Vacuuming completed.')
        else:
            logger.error('No DB file detected. Nothing to Vacuum!')
