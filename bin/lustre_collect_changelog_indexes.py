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


import os
import sys
import logging
import argparse
import signal
import time
import datetime


DESCRIPTION="""Collects changelog indexes on the given MDT and writes the following information into an unload file (separated in columns):
1) Timestamp of calculation.
2) Increase of the current index on the MDT.
3) Consumption number of the given changelog reader.
4) The delta between the current index and the changelog reader index.
"""

VERSION='1.0'

DELIMITER=';'
INTERVAL_SECONDS=300
UNLOAD_FILE='changelog_indexes.unl'

CURR_IDX_POS=0
CLOG_IDX_POS=1

run_cond=True


def validate( clog_users_file, interval ):
   
   if os.path.isfile( clog_users_file ):
      logging.debug( "Found the following changelog users file: " + clog_users_file )
   else:
      raise IOError( "The following changelog users file does not exist: " + clog_users_file )
   
   if interval < 1 or interval > 3600:
      raise ValueError( 'The specified interval must be a value beetween 1 to 3600 seconds!' )


def signal_handler( signal, frame ):
   
   logging.debug( "Catched SIGINT signal to stop the current script execution." )
   
   global run_cond
   run_cond = False


def get_current_timestamp():
   
   return datetime.datetime.now().strftime( '%Y-%m-%d %H:%M:%S' )


def read_indexes( clog_users_file, clog_reader ):

   if not clog_reader:
      raise RuntimeError('No changelog reader is specified!')
   
   current_index = None
   clog_index    = None

   with open( clog_users_file ) as clog_file:

      for line in clog_file:
         
         if "current index:" in line:

            current_index = line.split( ':' )[ 1 ].strip()
            
            if not current_index.isdigit():
               raise ValueError( "Retrieved current index is not a number: " + current_index )
         
         if clog_reader in line:
            clog_index = line.split( clog_reader )[ 1 ].strip()

         if current_index and clog_index:
            break
   
   if not current_index:
      raise RuntimeError( 'No current index has been set!' )
         
   if not clog_index:
      raise RuntimeError( "No index has been set for the changelog reader: " + clog_reader )
   
   return tuple( ( current_index, clog_index ) )


def calc_values( prev_curr_index, prev_clog_index, current_index, clog_index ):
   
   number_prev_curr_index = int( prev_curr_index )
   number_prev_clog_index = 0
   number_current_index   = int( current_index )
   number_clog_index      = 0
   
   if prev_clog_index:
      
      number_prev_clog_index = int( prev_clog_index )
      
      if number_prev_clog_index <= 0:
         raise ValueError( "Previous changelog index is less or equal 0!" )
   
   if clog_index:
      
      number_clog_index = int( clog_index )
      
      if number_clog_index <= 0:
         raise ValueError( "Changelog index is less or equal 0!" )
   
   if number_prev_curr_index <= 0:
      raise ValueError( "Previous current index is less or equal 0!" )
   
   if number_current_index <= 0:
      raise ValueError( "Current index is less or equal 0!" )
   
   current_index_diff = number_current_index - number_prev_curr_index
   
   new_line = str( current_index_diff ) + DELIMITER
   
   if prev_clog_index and clog_index:
      
      clog_index_diff = number_clog_index - number_prev_clog_index
      
      new_line += str( clog_index_diff ) + DELIMITER
      
      curr_and_clog_index_diff = number_current_index - number_clog_index
      
      new_line += str( curr_and_clog_index_diff )
   
   else:
      new_line += DELIMITER
   
   return new_line


def process_indexes( clog_users_file, clog_reader, previous_indexes ):

   timestamp = get_current_timestamp()

   indexes = read_indexes(clog_users_file, clog_reader)

   return ( timestamp + DELIMITER + calc_values(previous_indexes[CURR_IDX_POS], previous_indexes[CLOG_IDX_POS], indexes[CURR_IDX_POS], indexes[CLOG_IDX_POS]), indexes )


def init_logging( enable_debug ):

    if enable_debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig( level=log_level, format="%(asctime)s - %(levelname)s: %(message)s" )


def main():

   try:

      parser = argparse.ArgumentParser( description=DESCRIPTION )

      parser.add_argument( '-i', '--interval',         dest='interval',         type=int,  help="Specifies the collect interval in seconds (default: " + str( INTERVAL_SECONDS ) + " seconds).",                 default=INTERVAL_SECONDS )
      parser.add_argument( '-d', '--delimiter',        dest='delimiter',        type=str,  help="Defines the delimiter for unloaded indexes (default: " + DELIMITER + ").",                                      default=DELIMITER )
      parser.add_argument( '-m', '--mdt-name',         dest='mdt_name',         type=str,  help='Specifies the MDT name where to read the current index from (e.g. \'fs-MDT0000\').' )
      parser.add_argument( '-r', '--changelog-reader', dest='changelog_reader', type=str,  help='Specifies an additional changelog reader to read the index from (optional - e.g. \'cl1\').',                    default=None )
      parser.add_argument( '-f', '--unload-file',      dest='unload_file',      type=str,  help="Specifies the unload file where the collected information is saved (" + str( UNLOAD_FILE ) + ").",              default=UNLOAD_FILE )
      parser.add_argument( '--direct-flush',           dest='direct_flush',                help="If enabled after each collection interval a disk write flush is done of the collected values.",                 default=False, action='store_true' )
      parser.add_argument( '--capture-delta',          dest='capture_delta',               help="Prints the delta between the changelog consumer and the MDT index after one interval on the stdout and quits.", default=False, action='store_true' )
      parser.add_argument( '-v', '--version',                                              help="Prints the version of the script.",                                                                             default=False, action='store_true' )
      parser.add_argument('-D', '--enable-debug',      dest='enable_debug',                help='Enables logging of debug messages.',                                                                                           action='store_true' )


      args = parser.parse_args()

      if args.version:

         print( "Version: %s" % VERSION )

         os._exit(0)

      init_logging( args.enable_debug )

      logging.debug('START')

      if not args.mdt_name:
         raise RuntimeError('No MDT name was specified!')

      clog_users_file = "/proc/fs/lustre/mdd/" + args.mdt_name + "/changelog_users"

      clog_reader = args.changelog_reader

      validate( clog_users_file, args.interval )

      signal.signal( signal.SIGINT, signal_handler )

      global run_cond

      # Initial Run.
      previous_indexes = read_indexes( clog_users_file, clog_reader )
      time.sleep( args.interval )

      if args.capture_delta:

         fmt_value_str, indexes = process_indexes( clog_users_file, clog_reader, previous_indexes )

         print( fmt_value_str )

         print(int(fmt_value_str.split(DELIMITER)[3]))

         os._exit(0)

      with open( args.unload_file, 'a' ) as outfile:

         while( run_cond ):

            fmt_value_str, indexes = process_indexes( clog_users_file, clog_reader, previous_indexes )

            outfile.write( fmt_value_str + '\n' )

            if args.direct_flush:
               outfile.flush()

            previous_indexes = indexes

            time.sleep( args.interval )

      logging.debug( 'END' )

      os._exit(0)

   except Exception as e:

      exc_type, exc_obj, exc_tb = sys.exc_info()
      filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

      sys.stderr.write("Exception occurred: %s - %s (line: %s)\n" % (str(e), filename, exc_tb.tb_lineno))

      os._exit(1)


if __name__ == '__main__':
   main()