#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Gabriele Iannetti <g.iannetti@gsi.de>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import ConfigParser
import logging
import argparse
import MySQLdb
import sys
import os
import numpy as np
import collections
import datetime

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage

from email import Encoders

# Force matplotlib to not use any X window backend.
import matplotlib as mpl
mpl.use('Agg')

import matplotlib.pyplot as plt

from matplotlib import cm
from contextlib import closing
from numbers import Number
from decimal import Decimal


def main():

   parser = argparse.ArgumentParser( description='Creates report.' )
   parser.add_argument( '-f', '--config-file', dest='config_file', type=str, required=True, help='Path of the config file.' )
   parser.add_argument( '-D', '--enable-debug', dest='enable_debug', required=False, action='store_true', help='Enables debug log messages.' )
   args = parser.parse_args()
   
   if not os.path.isfile( args.config_file ):
      raise IOError( "The config file does not exist or is not a file: " + args.config_file )
   
   config = ConfigParser.ConfigParser()
   config.read( args.config_file )
   
   logging_level = logging.INFO
   
   if args.enable_debug:
      logging_level = logging.DEBUG

   logging.basicConfig( level=logging_level, format='%(asctime)s - %(levelname)s: %(message)s' )

   logging.info( 'START' )
   
   try:
      
      HOST      = config.get( 'mysqld', 'host' )
      USER      = config.get( 'mysqld', 'user' )
      PASSWD    = config.get( 'mysqld', 'password' )
      DATABASE  = config.get( 'mysqld', 'database' )
      
      DAYS      = int( config.get( 'report', 'days' ) )
      THRESHOLD = int ( config.get( 'report', 'threshold' ) )
      SAVE_DIR  = config.get( 'report', 'save_dir' )
      
      if not os.path.isdir( SAVE_DIR ):
         raise RuntimeError( "Directory does not exist for saving reports: %s" % SAVE_DIR )
      
      date_dict = dict()
      
      end_date = datetime.datetime.today()
      start_date = end_date - datetime.timedelta( days = DAYS )
      
      start_date_str = start_date.strftime( '%Y-%m-%d' )
      end_date_str   = end_date.strftime( '%Y-%m-%d' )
      
      logging.debug( "Start date is: %s" % start_date_str )
      logging.debug( "End date is: %s" % end_date_str )
      
      for i in range ( 0, DAYS + 1 ):
         
         calc_date = start_date + datetime.timedelta( days = i )
         calc_date_str = calc_date.strftime( '%Y-%m-%d' )
         
         date_dict[ calc_date_str ] = 0
      
      date_str = datetime.datetime.now().strftime( '%Y_%m_%d' )
      overview_report_path = SAVE_DIR + "/" + "ost_perf_test_overview_" + date_str + ".svg" 
      
      logging.debug( "Saving overview report file under: %s" % overview_report_path )
      
      with closing( MySQLdb.connect( host=HOST, user=USER, passwd=PASSWD, db=DATABASE ) ) as conn:
         with closing( conn.cursor() ) as cur:
            
            sql = "SELECT DATE(timepoint), COUNT(*) FROM OST_PERF_HISTORY WHERE date(timepoint) BETWEEN '" + start_date_str + "' AND '" + end_date_str + "' AND throughput <= " + str( THRESHOLD ) + " GROUP BY 1;"
            
            logging.debug( sql )
            
            cur.execute( sql )
            
            result = cur.fetchall()
            
            # Set dictionary for date and counts.
            for item in result:
               
               date  = item[0]
               count = item[1]
               
               date_str = str( date.strftime( '%Y-%m-%d' ) )
               
               if date_str in date_dict:
                  date_dict[ date_str ] = count
               
            sorted_short_dates = list()
            y                  = list()
            max_y              = 0
            
            for date in sorted( date_dict ):
               
               date_long = datetime.datetime.strptime( date, '%Y-%m-%d')
               sorted_short_dates.append( date_long.strftime( '%d.%m.' ) )
               
               count = date_dict[date]
               
               y.append( count )
               
               if count > max_y:
                  max_y = count
            
            main_title = "OST Write-Performance Tests on Lustre-Nyx"
            sub_title  = "(from " + start_date_str + " to " + end_date_str + " with average <= " + str ( THRESHOLD / 1000000 ) + " MB/s)"
            
            snapshot_timestamp      = datetime.datetime.now().strftime( '%Y-%m-%d - %X' )
            creation_timestamp_text = "Timestamp: " + snapshot_timestamp
            
            N = len( y )
            x = np.arange(N)
            
            width   = 1/1.5
            
            y_steps = 10
            
            if max_y > 100:
            
               divor   = 10
               quo     = ( max_y / 8 ) / divor
               y_steps = divor * quo
            
            fig, ax = plt.subplots()
            
            fig.suptitle( main_title, fontsize = 18 ) 
            
            ax.set_title( sub_title, fontsize = 14 )
            
            rect = ax.bar(x, y, width, color="blue") 
            
            ax.set_xlabel('Days')
            ax.set_ylabel('Count')
            
            ax.set_xticks( x + width / 2 )
            ax.set_yticks( np.arange( 0, max_y, y_steps ) )
            ax.set_xticklabels( sorted_short_dates )
            
            ax.text( 0, 0, creation_timestamp_text, fontsize = 8, verticalalignment = 'bottom', horizontalalignment = 'left', transform = fig.transFigure )
            
            fig.set_size_inches( 10, 8 )
            
            fig.savefig( overview_report_path, format='svg', dpi=200 )
      
      logging.info( 'END' )
      
      return 0
   
   except Exception as e:
      
      error_msg = str( e )
      
      logging.error( error_msg )


if __name__ == '__main__':
   main()
