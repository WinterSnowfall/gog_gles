#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 2.00
@date: 22/11/2020

Warning: Built for use with python 3.6+
'''

import sqlite3
import logging
import argparse
from os import path
from sys import argv
from datetime import datetime
from matplotlib import pyplot
from matplotlib import dates
from matplotlib import patches
from matplotlib.ticker import MaxNLocator
from matplotlib.ticker import ScalarFormatter

##global parameters init
current_date = datetime.now().strftime('%Y%m%d')

##logging configuration block
log_file_full_path = path.join('..', 'logs', 'gog_plot_gen.log')
logger_file_handler = logging.FileHandler(log_file_full_path, mode='w', encoding='utf-8')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler.setFormatter(logging.Formatter(logger_format))
logging.basicConfig(format=logger_format, level=logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
logger.addHandler(logger_file_handler)

##db configuration block
db_file_full_path = path.join('..', 'output_db', 'gog_visor.db')

##CONSTANTS
OPTIMIZE_QUERY = 'PRAGMA optimize'

ID_INTERVALS_LENGTH = 10000000
PNG_WIDTH_INCHES = 19.20
PNG_HEIGHT_INCHES = 10.80
CUTOFF_ID = 10
#set this date to match your initial products_scan run
CUTOFF_DATE = '1970-01-01'

def plot_id_timeline(mode, db_connection):
    pyplot.title('gog_visor - GOG product id detection timeline')
    window_title = f'gog_{mode}_{current_date}'
    pyplot.gcf().canvas.set_window_title(window_title)
    pyplot.ylabel('id')
    pyplot.xlabel('detection date')
    pyplot.gcf().set_size_inches(PNG_WIDTH_INCHES, PNG_HEIGHT_INCHES)
    pyplot.gca().xaxis.set_major_formatter(dates.DateFormatter('%m/%Y'))
    pyplot.gca().xaxis.set_major_locator(dates.MonthLocator())
    y_formatter = ScalarFormatter(useOffset=False)
    y_formatter.set_scientific(False)
    pyplot.gca().yaxis.set_major_formatter(y_formatter)
    pyplot.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    pyplot.gca().grid(True)
    
    red_labels = 0
    blue_labels = 0
    green_labels = 0
    
    #in the interest of decompressing the chart, ignore the first 'CUTOFF_ID' IDs
    db_cursor = db_connection.execute('SELECT gp_int_added, gp_id, gp_game_type FROM gog_products '
                                      'WHERE gp_id > ? AND gp_int_added > ? ORDER BY 1', (CUTOFF_ID, CUTOFF_DATE))
    for row in db_cursor:
        plot_label = None
        
        current_date_string = row[0]
        logger.debug(f'current_date: {current_date_string}')
        current_id = row[1]
        logger.debug(f'current_id: {current_id}')
        current_game_type = row[2]
        logger.debug(f'current_game_type: {current_game_type}')
        
        if current_game_type == 'game':
            plot_point = 'r.'
            if red_labels == 0:
                plot_label = 'Type: game'
            red_labels+=1
        elif current_game_type == 'dlc':
            plot_point = 'b.'
            if blue_labels == 0:
                plot_label = 'Type: dlc'
            blue_labels+=1
        elif current_game_type == 'pack':
            plot_point = 'g.'
            if green_labels == 0:
                plot_label = 'Type: pack'
            green_labels+=1
        
        pyplot.plot(datetime.strptime(current_date_string, '%Y-%m-%d %H:%M:%S.%f').date(), 
                    current_id, plot_point, label=plot_label)
    
    pyplot.gcf().autofmt_xdate()
    pyplot.legend(bbox_to_anchor=(1, 1.11), loc=1, borderaxespad=0.)
    
    pyplot.ioff()
    pyplot.savefig(path.join('..', 'output_plot', ''.join((window_title, '.png'))))
    #uncomment for debugging purposes only
    #pyplot.show()
        
def plot_id_distribution(interval, mode, db_connection):
    window_title = f'gog_{mode}_{current_date}'
    pyplot.gcf().canvas.set_window_title(window_title)
    pyplot.ylabel('#ids')
    pyplot.xlabel(f'intervals of {interval} ids')
    pyplot.gcf().set_size_inches(PNG_WIDTH_INCHES, PNG_HEIGHT_INCHES)
    x_formatter = ScalarFormatter(useOffset=False)
    x_formatter.set_scientific(False)
    pyplot.gca().xaxis.set_major_formatter(x_formatter)
    pyplot.gca().xaxis.set_major_locator(MaxNLocator(integer=True))
    y_formatter = ScalarFormatter(useOffset=False)
    y_formatter.set_scientific(False)
    pyplot.gca().yaxis.set_major_formatter(y_formatter)
    pyplot.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
    pyplot.gca().grid(True)
    
    current_interval = interval
    current_interval_ids = 0
    total_ids = 0
    
    current_interval_list = []
    current_interval_ids_list = []
    
    colors = []
    
    if mode == 'distribution':
        pyplot.title(f'gog_visor - id distribution per intervals of {interval} ids (all ids)')
        db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id > ? ORDER BY 1', (CUTOFF_ID, ))
    else:
        pyplot.title(f'gog_visor - id distribution per intervals of {interval} ids (incremental ids)')
        db_cursor = db_connection.execute('SELECT gp_id FROM gog_products WHERE gp_id > ? AND gp_int_added > ? ORDER BY 1',
                                          (CUTOFF_ID, CUTOFF_DATE))
        
    for row in db_cursor:
        total_ids+=1
        current_id = row[0]
        logger.debug(f'current_id: {current_id}.')
        id_not_processed = True
        
        while id_not_processed:
            if current_id <= current_interval:
                current_interval_ids+=1
                id_not_processed = False
            else:
                logger.debug(f'current_interval: {current_interval}.')
                logger.debug(f'current_interval_ids: {current_interval_ids}.')
                if current_interval_ids > 0:
                    #entries between x-1 and x will be listed under interval x-1
                    current_interval_list.append(current_interval-interval)
                    current_interval_ids_list.append(current_interval_ids)
                current_interval+=interval
                current_interval_ids = 0
        
    #also add the last interval which does not make the else branch
    logger.debug(f'current_interval: {current_interval}.')
    logger.debug(f'current_interval_ids: {current_interval_ids}.')
    current_interval_list.append(current_interval-interval)
    current_interval_ids_list.append(current_interval_ids)
    
    logger.debug(f'current_interval_list size: {len(current_interval_list)}.')
    logger.debug(f'current_interval_ids size: {len(current_interval_ids_list)}.')
    
    id_count_average = sum(current_interval_ids_list)/len(current_interval_ids_list)
    clearly_above_average = id_count_average * 1.5
    clearly_below_average = id_count_average * 0.5
        
    for ids_value in current_interval_ids_list:
        if ids_value > clearly_above_average:
            colors.append('m')
        elif ids_value < clearly_below_average:
            colors.append('c')
        else:
            colors.append('b')

    pyplot.bar(current_interval_list, current_interval_ids_list, width=interval, color=colors)
        
    magenta_patch = patches.Patch(color='m', label='Above average (by more than 50%)')
    blue_patch = patches.Patch(color='b', label='Within 50% (+/-) of average value')
    cyan_patch = patches.Patch(color='c', label='Below average (by more than 50%)')
    pyplot.legend(handles=[magenta_patch, blue_patch, cyan_patch], 
                  bbox_to_anchor=(1, 1.10), loc=1, borderaxespad=0.)
    
    pyplot.ioff()
    pyplot.savefig(path.join('..', 'output_plot', ''.join((window_title, '.png'))))
    #uncomment for debugging purposes only
    #pyplot.show()

##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description=('GOG plot generation (part of gog_visor) - a script to generate GOG-related '
                                              'statistics and charts.'))

group = parser.add_mutually_exclusive_group()
group.add_argument('-t', '--timeline', help='Generate id detection timeline chart', action='store_true')
group.add_argument('-d', '--distribution', help='Generate the id distribution chart (all ids)', action='store_true')
group.add_argument('-i', '--incremental', help='Generate the id distribution chart (incremental ids)', action='store_true')

args = parser.parse_args()

logger.info('*** Running PLOT GENERATION script ***')

#select a default plot mode if no command line switch is specified
plot_mode = 'timeline'

#detect any parameter overrides and set the scan_mode accordingly
if len(argv) > 1:
    logger.info('Command-line parameter mode override detected.')
    
    if args.timeline:
        plot_mode = 'timeline'
    elif args.distribution:
        plot_mode = 'distribution'
    elif args.incremental:
        plot_mode = 'incremental'

if plot_mode == 'timeline':
    logger.info('--- Running in ID TIMELINE mode ---')
    
    with sqlite3.connect(db_file_full_path) as db_connection:
        plot_id_timeline(plot_mode, db_connection)
        
        logger.debug('Running PRAGMA optimize...')
        db_connection.execute(OPTIMIZE_QUERY)
    
elif plot_mode == 'distribution':
    logger.info('--- Running in ID DISTRIBUTION mode (all) ---')
    
    with sqlite3.connect(db_file_full_path) as db_connection:
        plot_id_distribution(ID_INTERVALS_LENGTH, plot_mode, db_connection)
        
        logger.debug('Running PRAGMA optimize...')
        db_connection.execute(OPTIMIZE_QUERY)
    
elif plot_mode == 'incremental':
    logger.info('--- Running in ID DISTRIBUTION mode (incremental) ---')
    
    with sqlite3.connect(db_file_full_path) as db_connection:
        plot_id_distribution(ID_INTERVALS_LENGTH, plot_mode, db_connection)
        
        logger.debug('Running PRAGMA optimize...')
        db_connection.execute(OPTIMIZE_QUERY)

logger.info('All done! Exiting...')

##main thread end
