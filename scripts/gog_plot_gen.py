#!/usr/bin/env python3
'''
@author: Winter Snowfall
@version: 1.60
@date: 10/10/2020

Warning: Built for use with python 3.6+
'''

import sqlite3
import datetime
import logging
import argparse
from logging.handlers import RotatingFileHandler
from os import path
from sys import argv
from matplotlib import pyplot
from matplotlib import dates
from matplotlib import patches
from matplotlib.ticker import MaxNLocator
from matplotlib.ticker import ScalarFormatter

##global parameters init
current_date = datetime.datetime.date(datetime.datetime.now()).strftime('%Y%m%d')

##logging configuration block
log_file_full_path = path.join('..', 'logs', 'gog_plot_gen.log')
logger_format = '%(asctime)s %(levelname)s >>> %(message)s'
logger_file_handler = RotatingFileHandler(log_file_full_path, maxBytes=8388608, backupCount=1, encoding='utf-8')
logger_file_formatter = logging.Formatter(logger_format)
logger_file_handler.setFormatter(logger_file_formatter)
logging.basicConfig(format=logger_format, level=logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL
logger = logging.getLogger(__name__)
logger.addHandler(logger_file_handler)

##db configuration block
db_file_full_path = path.join('..', 'output_db', 'gog_visor.db')

##CONSTANTS
OPTIMIZE_QUERY = 'PRAGMA optimize'

def plot_id_history():
    with sqlite3.connect(db_file_full_path) as db_connection:
        pyplot.title('gog_visor - New GOG product id detections over time')
        window_title = f'gog_idd_{current_date}'
        pyplot.gcf().canvas.set_window_title(window_title)
        pyplot.ylabel('id')
        pyplot.xlabel('detection date')
        pyplot.gcf().set_size_inches(19.20, 10.80)
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
 
        #in the interest of decompressing the chart, ignore the first 10 ids (which are in use)
        db_cursor = db_connection.execute('SELECT gp_int_added, gp_id, gp_game_type FROM gog_products where gp_id > 10 ORDER BY 1')
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
            else:
                plot_point = 'k.'
            
            pyplot.plot(datetime.datetime.strptime(current_date_string, '%Y-%m-%d %H:%M:%S.%f').date(), current_id, plot_point, label=plot_label)    

        pyplot.gcf().autofmt_xdate()
        pyplot.legend(bbox_to_anchor=(1, 1.11), loc=1, borderaxespad=0.)
        
        pyplot.ioff()
        pyplot.savefig(path.join('..', 'output_plot', window_title))
        #pyplot.show()
        
        logger.debug('Running PRAGMA optimize...')
        db_connection.execute(OPTIMIZE_QUERY)
        
def plot_id_distribution(interval, mode):
    with sqlite3.connect(db_file_full_path) as db_connection:
        db_cursor = db_connection.cursor()
        
        if mode == 'dist':
            pyplot.ylabel('#ids')
        elif mode == 'prob':
            pyplot.ylabel('probability')
            
        pyplot.xlabel(f'intervals of {interval} ids')

        pyplot.gcf().set_size_inches(19.20, 10.80)
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
        current_probability = 0
        current_interval_probability = []
        interval_counter = 0
        
        colors = []
        
        if mode == 'dist':
            pyplot.title(f'gog_visor - id distribution per intervals of {interval} ids (all ids)')
            window_title = f'gog_idt_{current_date}'
            pyplot.gcf().canvas.set_window_title(window_title)
            db_cursor.execute('SELECT gp_id FROM gog_products where gp_id > 10 ORDER BY 1')
        elif mode == 'prob':
            pyplot.title(f'gog_visor - discrete id probability per intervals of {interval} ids (all ids)')
            window_title = f'gog_dpy_{current_date}'
            pyplot.gcf().canvas.set_window_title(window_title)
            db_cursor.execute('SELECT gp_id FROM gog_products where gp_id > 10 ORDER BY 1')
            
        for row in db_cursor:
            total_ids+=1
            current_id = row[0]
            logger.debug(f'current_id: {current_id}')
            id_not_processed = True
            
            while id_not_processed:
                if current_id <= current_interval:
                    current_interval_ids+=1
                    id_not_processed = False
                else:
                    logger.debug(f'current_interval: {current_interval}')
                    logger.debug(f'current_interval_ids: {current_interval_ids}')
                    if current_interval_ids > 0:
                        #entries between x-1 and x will be listed under interval x-1
                        current_interval_list.append(current_interval-interval)
                        current_interval_ids_list.append(current_interval_ids)
                    current_interval+=interval
                    current_interval_ids = 0
            
        #also add the last interval which does not make the else branch
        logger.debug(f'current_interval: {current_interval}')
        logger.debug(f'current_interval_ids: {current_interval_ids}')
        current_interval_list.append(current_interval-interval)
        current_interval_ids_list.append(current_interval_ids)
        
        logger.debug(f'current_interval_list size: {len(current_interval_list)}')
        logger.debug(f'current_interval_ids size: {len(current_interval_ids_list)}')
        
        id_count_average = sum(current_interval_ids_list)/len(current_interval_ids_list)
        clearly_above_average = id_count_average + id_count_average/1.5
        clearly_below_average = id_count_average - id_count_average/1.5
        
        if mode == 'prob':
            logger.info('Discrete id probability values per interval:')
            
        for ids_value in current_interval_ids_list:
            if mode == 'dist':
                if ids_value >= clearly_above_average:
                    colors.append('m')
                elif ids_value <= clearly_below_average:
                    colors.append('c')
                else:
                    colors.append('b')
            elif mode == 'prob':
                current_probability = ids_value/total_ids
                logger.info(f'{current_interval_list[interval_counter]}: {current_probability}')
                current_interval_probability.append(current_probability)
                interval_counter+=1

        if mode == 'dist':
            pyplot.bar(current_interval_list, current_interval_ids_list, width=interval, color=colors)
        
        elif mode == 'prob':
            id_probability_average = 1/len(current_interval_probability)
            clearly_above_average = id_probability_average + id_probability_average/1.5
            clearly_below_average = id_probability_average - id_probability_average/1.5
            
            for probability in current_interval_probability:
                if probability >= clearly_above_average:
                    colors.append('m')
                elif probability <= clearly_below_average:
                    colors.append('c')
                else:
                    colors.append('b')
            
            pyplot.bar(current_interval_list, current_interval_probability, width=interval, color=colors)
            
        magenta_patch = patches.Patch(color='m', label='Above average (by more than 2/3)')
        blue_patch = patches.Patch(color='b', label='Within 2/3 of average value')
        cyan_patch = patches.Patch(color='c', label='Below average (by more than 2/3)')
        pyplot.legend(handles=[magenta_patch, blue_patch, cyan_patch])
        
        pyplot.ioff()
        pyplot.savefig(path.join('..', 'output_plot', window_title))
        #pyplot.show()
        
        logger.debug('Running PRAGMA optimize...')
        db_connection.execute(OPTIMIZE_QUERY)

##main thread start

#added support for optional command-line parameter mode switching
parser = argparse.ArgumentParser(description='GOG plot generation (part of gog_visor) - a script to generate GOG-related \
                                              statistics and charts.')

group = parser.add_mutually_exclusive_group()
group.add_argument('-n', '--new', help='Generate id history chart', action='store_true')
group.add_argument('-d', '--distribution', help='Generate the id distribution chart', action='store_true')
group.add_argument('-p', '--probability', help='Generate the id probability chart', action='store_true')

args = parser.parse_args()

logger.info('*** Running PLOT GENERATION script ***')

#select a default plot mode if no command line switch is specified
plot_mode = 'new'

#detect any parameter overrides and set the scan_mode accordingly
if len(argv) > 1:
    logger.info('Command-line parameter mode override detected.')
    
    if args.new:
        plot_mode = 'new'
    elif args.distribution:
        plot_mode = 'distribution'
    elif args.probability:
        plot_mode = 'probability'

if plot_mode == 'new':
    logger.info('--- Running in ID HISTORY mode ---')
    plot_id_history()
    
elif plot_mode == 'distribution':
    logger.info('--- Running in ID DISTRIBUTION mode (all) ---')
    plot_id_distribution(10000000, 'dist')
    
elif plot_mode == 'probability':
    logger.info('--- Running in ID PROBABILITY mode (all) ---')
    plot_id_distribution(10000000, 'prob')

logger.info('All done! Exiting...')

##main thread end
