#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 3.80
@date: 12/06/2023

Warning: Built for use with python 3.6+
'''

import sqlite3
import logging
import signal
import argparse
import os
from sys import argv
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot
from matplotlib import dates
from matplotlib import gridspec
from matplotlib.ticker import MaxNLocator
from matplotlib.ticker import AutoMinorLocator
from matplotlib.ticker import ScalarFormatter

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
OPTIMIZE_QUERY = 'PRAGMA optimize'

PNG_WIDTH_INCHES = 21.60
PNG_HEIGHT_INCHES = 10.80
ID_INTERVAL_LENGTH = 10000000
MAX_ID = 2147483647
CUTOFF_ID = 10
# set this date to match your initial products_scan run
CUTOFF_DATE = '1970-01-01'

DEFAULT_CHART_COLORS = ('tab:orange', 'tab:blue', 'tab:green')
SUNSET_CHART_COLORS = ('purple', 'slateblue', 'goldenrod')
FIRE_CHART_COLORS = ('darkred', 'darkorange', 'gold')
DARKNESS_CHART_COLORS = ('black', 'slategray', 'lightgray')

CHART_LABELS = ('Type: game', 'Type: dlc', 'Type: pack')

TIMELINE_FIELDS = ('id', 'release date')
TIMELINE_Y_OFFSETS = (ID_INTERVAL_LENGTH, timedelta(weeks=+28))

def sigterm_handler(signum, frame):
    logger.debug('Stopping scan due to SIGTERM...')
    
    raise SystemExit(0)

def sigint_handler(signum, frame):
    logger.debug('Stopping scan due to SIGINT...')
    
    raise SystemExit(0)

def plot_id_timeline(mode, timeline_field, plot_date, db_connection):
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)
    
    pyplot.suptitle('gog_gles - GOG product id detection timeline '
                   f'(with {timeline_field} / detection date histograms) - {plot_date}')
    timeline_field_nowhitespace = timeline_field.replace(' ','_')
    window_title = f'gog_{mode}_{timeline_field_nowhitespace}'
    pyplot.gcf().canvas.set_window_title(window_title)
    pyplot.gcf().set_size_inches(PNG_WIDTH_INCHES, PNG_HEIGHT_INCHES)
    
    # generate gridspec and subplots on a 1/6 ratio
    gspec = gridspec.GridSpec(6, 6)
    top_hist = pyplot.subplot(gspec[0, 1:])
    side_hist = pyplot.subplot(gspec[1:, 0])
    dist_chart = pyplot.subplot(gspec[1:, 1:], sharey=side_hist, sharex=top_hist)
    # set locators for the x axis
    pyplot.gca().xaxis.set_major_locator(dates.YearLocator())
    pyplot.gca().xaxis.set_minor_locator(dates.MonthLocator())
    # set locators for the y axis
    if timeline_field == TIMELINE_FIELDS[0]:
        y_formatter = ScalarFormatter(useOffset=False)
        y_formatter.set_scientific(False)
        pyplot.gca().yaxis.set_major_formatter(y_formatter)
        pyplot.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
        pyplot.gca().yaxis.set_minor_locator(AutoMinorLocator())
    elif timeline_field == TIMELINE_FIELDS[1]:
        pyplot.gca().yaxis.set_major_locator(dates.YearLocator(3))
        pyplot.gca().yaxis.set_minor_locator(dates.YearLocator())
    # enable dotted grid lines
    pyplot.gca().grid(True, ls='--')
    top_hist.get_xaxis().grid(True, ls='--')
    side_hist.get_yaxis().grid(True, ls='--')
    # ensure grid lines are drawn behind plot lines/points
    pyplot.gca().set_axisbelow(True)
    top_hist.set_axisbelow(True)
    side_hist.set_axisbelow(True)
    
    month_list = set()
    game_list = []
    dlc_list = []
    pack_list = []
    game_detection_date_list = []
    dlc_detection_date_list = []
    pack_detection_date_list = []
    
    # in the interest of decompressing the chart, ignore the first 'CUTOFF_ID' IDs
    db_cursor = db_connection.execute('SELECT gp_int_added, gp_id, gp_game_type, gp_v2_global_release_date FROM gog_products '
                                      'WHERE gp_id > ? AND gp_int_added > ? AND gp_int_delisted IS NULL '
                                      'ORDER BY 1', (CUTOFF_ID, CUTOFF_DATE))
    
    for row in db_cursor:
        current_date = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S.%f').date()
        logger.debug(f'current_date: {current_date}')
        current_id = row[1]
        logger.debug(f'current_id: {current_id}')
        current_game_type = row[2]
        logger.debug(f'current_game_type: {current_game_type}')
        try:
            current_release_date = datetime.strptime(row[3], '%Y-%m-%dT%H:%M:%S%z').date()
        except TypeError:
            current_release_date = None
        logger.debug(f'current_release_date: {current_release_date}')
        
        if current_game_type == 'game':
            if timeline_field == TIMELINE_FIELDS[0]:
                game_detection_date_list.append(current_date)
                game_list.append(current_id)
            elif timeline_field == TIMELINE_FIELDS[1]:
                if current_release_date is not None:
                    game_detection_date_list.append(current_date)
                    game_list.append(current_release_date.replace(day=1))
        elif current_game_type == 'dlc':
            if timeline_field == TIMELINE_FIELDS[0]:
                dlc_detection_date_list.append(current_date)
                dlc_list.append(current_id)
            elif timeline_field == TIMELINE_FIELDS[1]:
                if current_release_date is not None:
                    dlc_detection_date_list.append(current_date)
                    dlc_list.append(current_release_date.replace(day=1))
        elif current_game_type == 'pack':
            if timeline_field == TIMELINE_FIELDS[0]:
                pack_detection_date_list.append(current_date)
                pack_list.append(current_id)
            elif timeline_field == TIMELINE_FIELDS[1]:
                if current_release_date is not None:
                    pack_detection_date_list.append(current_date)
                    pack_list.append(current_date.replace(day=1))
        
        # add unique months to a set and base the histogram bins size on set length
        month_list.add(current_date.replace(day=1))
    
    if timeline_field == TIMELINE_FIELDS[0]:
        RANGE_MIN = min(*game_list, *dlc_list, *pack_list)
        RANGE_MAX = MAX_ID
        RANGE = range(RANGE_MIN, RANGE_MAX, ID_INTERVAL_LENGTH)
    elif timeline_field == TIMELINE_FIELDS[1]:
        RANGE_MIN = min(*game_list, *dlc_list, *pack_list)
        RANGE_MAX = max(*game_list, *dlc_list, *pack_list)
        RANGE = RANGE_MAX.year - RANGE_MIN.year
    
    # generate main scatter chart
    dist_chart.scatter(game_detection_date_list, game_list, s=5, c=CHART_COLORS[0])
    dist_chart.scatter(dlc_detection_date_list, dlc_list, s=5, c=CHART_COLORS[1])
    dist_chart.scatter(pack_detection_date_list, pack_list, s=5, c=CHART_COLORS[2])
    
    # generate top histogram (detection dates)
    top_hist.hist([game_detection_date_list, dlc_detection_date_list, pack_detection_date_list], 
                  bins=len(month_list), color=CHART_COLORS, stacked=True, label=CHART_LABELS)
    top_hist.legend(loc=2, ncol=3)
    
    # generate side histogram (ids)
    side_hist.hist([game_list, dlc_list, pack_list], orientation='horizontal', 
                   bins=RANGE, color=CHART_COLORS, stacked=True)
    side_hist.invert_xaxis()
    
    # hide duplicate tick labels
    pyplot.setp(dist_chart.get_yticklabels(), visible=False)
    pyplot.setp(top_hist.get_xticklabels(), visible=False)
    
    # set proper limits for x/y axes
    dist_chart.set_xlim(min(*game_detection_date_list, *dlc_detection_date_list, *pack_detection_date_list) - timedelta(weeks=+1), 
                    max(*game_detection_date_list, *dlc_detection_date_list, *pack_detection_date_list) + timedelta(weeks=+1))
    if timeline_field == TIMELINE_FIELDS[0]:
        dist_chart.set_ylim(RANGE_MIN - TIMELINE_Y_OFFSETS[0], RANGE_MAX + TIMELINE_Y_OFFSETS[0])
    elif timeline_field == TIMELINE_FIELDS[1]:
        dist_chart.set_ylim(RANGE_MIN - TIMELINE_Y_OFFSETS[1], RANGE_MAX + TIMELINE_Y_OFFSETS[1])
    # reduce exterior padding
    pyplot.tight_layout(pad=5, h_pad=1, w_pad=0)
    
    pyplot.ioff()
    pyplot.savefig(os.path.join('..', 'output_plot', window_title + '.png'))
    # uncomment for debugging purposes only
    #pyplot.show()

def plot_id_distribution(mode, plot_date, db_connection):
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)
    
    window_title = f'gog_{mode}'
    pyplot.gcf().canvas.set_window_title(window_title)
    pyplot.gcf().set_size_inches(PNG_WIDTH_INCHES, PNG_HEIGHT_INCHES)
    
    x_formatter = ScalarFormatter(useOffset=False)
    x_formatter.set_scientific(False)
    pyplot.gca().xaxis.set_major_formatter(x_formatter)
    pyplot.gca().xaxis.set_major_locator(MaxNLocator(12, integer=True))
    pyplot.gca().xaxis.set_minor_locator(AutoMinorLocator(5))
    y_formatter = ScalarFormatter(useOffset=False)
    y_formatter.set_scientific(False)
    pyplot.gca().yaxis.set_major_formatter(y_formatter)
    pyplot.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    pyplot.gca().yaxis.set_minor_locator(AutoMinorLocator())
    
    # enable dotted grid lines
    pyplot.gca().grid(True, ls='--')
    # ensure grid lines are drawn behind plot lines/points
    pyplot.gca().set_axisbelow(True)
    
    game_id_list = []
    dlc_id_list = []
    pack_id_list = []
    
    if mode == 'distribution':
        pyplot.suptitle(f'gog_gles - id distribution per intervals of {ID_INTERVAL_LENGTH} '
                        f'ids (all ids) - {plot_date}')
        db_cursor = db_connection.execute('SELECT gp_id, gp_game_type FROM gog_products WHERE gp_id > ? '
                                          'AND gp_int_delisted IS NULL ORDER BY 1', (CUTOFF_ID,))
    else:
        pyplot.suptitle(f'gog_gles - id distribution per intervals of {ID_INTERVAL_LENGTH} '
                        f'ids (incremental ids) - {plot_date}')
        db_cursor = db_connection.execute('SELECT gp_id, gp_game_type FROM gog_products WHERE gp_id > ? '
                                          'AND gp_int_added > ? AND gp_int_delisted IS NULL ORDER BY 1',
                                          (CUTOFF_ID, CUTOFF_DATE))
    
    for row in db_cursor:
        current_id = row[0]
        logger.debug(f'current_id: {current_id}')
        current_game_type = row[1]
        logger.debug(f'current_game_type: {current_game_type}')
        
        if current_game_type == 'game':
            game_id_list.append(current_id)
        elif current_game_type == 'dlc':
            dlc_id_list.append(current_id)
        elif current_game_type == 'pack':
            pack_id_list.append(current_id)
    
    min_id = min(*game_id_list, *dlc_id_list, *pack_id_list)
    
    #g enerate id histogram
    pyplot.hist([game_id_list, dlc_id_list, pack_id_list], 
                bins=range(min_id - ID_INTERVAL_LENGTH, MAX_ID + ID_INTERVAL_LENGTH, ID_INTERVAL_LENGTH), 
                color=CHART_COLORS, stacked=True, label=CHART_LABELS)
    pyplot.legend(bbox_to_anchor=(1, 1.05), loc=1, borderaxespad=0., ncol=3)
    
    # set proper limits for x axis
    pyplot.gca().set_xlim(min_id - ID_INTERVAL_LENGTH, MAX_ID + ID_INTERVAL_LENGTH)
    # reduce exterior padding
    pyplot.tight_layout(pad=5, h_pad=1, w_pad=0)
    
    pyplot.ioff()
    pyplot.savefig(os.path.join('..', 'output_plot', window_title + '.png'))
    # uncomment for debugging purposes only
    #pyplot.show()

if __name__ == "__main__":
    # catch SIGTERM and exit gracefully
    signal.signal(signal.SIGTERM, sigterm_handler)
    # catch SIGINT and exit gracefully
    signal.signal(signal.SIGINT, sigint_handler)
    
    parser = argparse.ArgumentParser(description=('GOG plot generation (part of gog_gles) - a script to generate GOG-related '
                                                  'statistics and charts.'))
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', '--timeline', help='Generate id detection timeline chart', action='store_true')
    group.add_argument('-d', '--distribution', help='Generate the id distribution chart (all ids)', action='store_true')
    group.add_argument('-i', '--incremental', help='Generate the id distribution chart (incremental ids)', action='store_true')
    
    parser.add_argument('-c', '--colors', help='Pick a color theme between: default, sunset, fire, darkness')
    
    parser.add_argument('-f', '--field', help='Pick a distribution field for timelines between: id, release')
    
    args = parser.parse_args()
    
    logger.info('*** Running PLOT GENERATION script ***')
    
    CHART_COLORS = DEFAULT_CHART_COLORS
    # select a default plot mode if no command line switch is specified
    plot_mode = 'timeline'
    # select a timeline field if none is specified
    timeline_field = TIMELINE_FIELDS[0];
    
    # detect any parameter overrides and set the scan_mode accordingly
    if len(argv) > 1:
        logger.info('Command-line parameter mode override detected.')
        
        if args.timeline:
            plot_mode = 'timeline'
        elif args.distribution:
            plot_mode = 'distribution'
        elif args.incremental:
            plot_mode = 'incremental'
        
        if args.colors == 'sunset':
            CHART_COLORS = SUNSET_CHART_COLORS
        elif args.colors == 'fire':
            CHART_COLORS = FIRE_CHART_COLORS
        elif args.colors == 'darkness':
            CHART_COLORS = DARKNESS_CHART_COLORS
            
        if args.field == 'release':
            timeline_field = TIMELINE_FIELDS[1]
            
    date_now = datetime.now().strftime('%d/%m/%Y')
    
    if plot_mode == 'timeline':
        try:
            logger.info('--- Running in ID TIMELINE mode ---')
            
            with sqlite3.connect(DB_FILE_PATH) as db_connection:
                plot_id_timeline(plot_mode, timeline_field, date_now, db_connection)
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            logger.info('Stopping timeline generation...')
    
    elif plot_mode == 'distribution':
        try:
            logger.info('--- Running in ID DISTRIBUTION mode (all) ---')
            
            with sqlite3.connect(DB_FILE_PATH) as db_connection:
                plot_id_distribution(plot_mode, date_now, db_connection)
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            logger.info('Stopping distribution generation...')
    
    elif plot_mode == 'incremental':
        try:
            logger.info('--- Running in ID DISTRIBUTION mode (incremental) ---')
            
            with sqlite3.connect(DB_FILE_PATH) as db_connection:
                plot_id_distribution(plot_mode, date_now, db_connection)
                
                logger.debug('Running PRAGMA optimize...')
                db_connection.execute(OPTIMIZE_QUERY)
        
        except SystemExit:
            logger.info('Stopping incremental generation...')
    
    logger.info('All done! Exiting...')
