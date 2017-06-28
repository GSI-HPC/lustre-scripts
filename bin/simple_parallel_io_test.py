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
import argparse
import logging
import sys
import os
import datetime, time
import commands
import os.path
import re
import random
import multiprocessing
import subprocess

from contextlib import closing
from ctypes import c_char


TIMEOUT = 90


def write_to_file( filepath, block_size, length, value ):
   
   value_char = str( value )[0]
   
   count = length / block_size
   total_size = 0
   
   if count < 1:
      raise RuntimeError( "Count of write_to_file is less than 1!" )
   
   payload = "".join( value_char for i in xrange( block_size ) )
   
   start_time = time.time() * 1000.0
   
   for i in xrange( count ):
      
      start_time = time.time() * 1000.0
      
      with open( filepath, 'a' ) as f:
         
         f.write( payload )
         
         total_size += block_size
   
      end_time = time.time() * 1000.0
      duration = end_time - start_time
   
      duration_sec = duration / 1000.0
      throughput   = round( ( block_size / duration_sec ) / 1000000.0, 2 )
      
      logging.debug( "Block-Write complete: %s -  Length: %s MB - Duration: %s seconds - Throughput: %s MB/s" % ( filepath, ( block_size / 1000000.0 ), round( duration_sec, 2 ), throughput ) )
   
   # Append the rest of bytes to write... 
   off_payload_size = length % block_size
   
   if off_payload_size > 0:
      
      off_payload = "".join( value_char for i in xrange( off_payload_size ) )
      
      with open( filepath, 'a' ) as f:
         
         f.write( off_payload )
         
         logging.debug( "Off payload has been written..." )
   
   end_time = time.time() * 1000.0
   duration = end_time - start_time
   
   duration_sec = duration / 1000.0
   throughput   = round( ( length / duration_sec ) / 1000000.0, 2 )
   
   logging.info( "File-Write complete: %s -  Length: %s MB - Duration: %s seconds - Throughput: %s MB/s" % ( filepath, ( length / 1000000.0 ), round( duration_sec, 2 ), throughput ) )
   
   logging.debug( "Total size written: %s" % str( total_size ) )
   
   return 0


def read_from_file( filepath, block_size ):
   
   start_time = time.time() * 1000.0
   
   total_length = 0
   
   with open( filepath ) as f:
      
      read_bytes = f.read( block_size )
      
      total_length += len( read_bytes )
      
      while read_bytes:
         
         read_bytes = f.read( block_size )
         
         total_length += len( read_bytes )
         
   end_time     = time.time() * 1000.0
   duration     = end_time - start_time
   duration_sec = duration / 1000.0
   
   throughput   = round( ( total_length / duration_sec ) / 1000000.0, 2 )
   
   logging.info( "Reading a line from file: %s took: %s seconds with throughput: %s MB/s of total length: %s" % ( filepath, round( duration_sec, 2 ), throughput, total_length ) )
   
   return 0
   


def main():

   parser = argparse.ArgumentParser( description='Testing writes on a file.' )
   parser.add_argument( '-f', '--target-file',  dest='target_file',  required=True,  type=str, help='Target file.' )
   parser.add_argument( '-s', '--total-size',   dest='total_size',   required=False, type=int, help='Total file size.',           default=10000000 )
   parser.add_argument( '-b', '--block-size',   dest='block_size',   required=False, type=int, help='Block size.',                default=1000 )
   parser.add_argument( '-r', '--reader-count', dest='reader_count', required=False, type=int, help='Count of reader processes.', default=1 )
   parser.add_argument( '-w', '--writer-count', dest='writer_count', required=False, type=int, help='Count of writer processes.', default=1 )
   parser.add_argument( '-D', '--enable-debug', dest='enable_debug', required=False, action='store_true', help='Enables debug log messages.' )
   
   args = parser.parse_args()
   
   logging_level = logging.INFO
   
   if args.enable_debug:
      logging_level = logging.DEBUG

   logging.basicConfig( level=logging_level, format='%(asctime)s - %(levelname)s: %(message)s' )
   
   try:
      
      logging.info( 'START' )
      
      logging.info( "Total size is: %s MB" % str( round( args.total_size / 1000000, 2 ) ) )
      logging.info( "Block size is: %s MB" % str( round( args.block_size / 1000000, 2 ) ) )
      
      start_time = time.time()
      
      block_size = args.block_size
      
      # Check if writer should be used...
      if args.writer_count and args.writer_count > 0:
         
         writer_list = list()
         
         length = args.total_size / args.writer_count
      
         # Prepare writer
         for i in range( args.writer_count ):
            
            writer_name   = "writer_" + str( i )
            writer_handle = multiprocessing.Process( name=writer_name, target=write_to_file, args=( args.target_file, block_size, length, i ) )
            writer_list.append( writer_handle )
            
         # Start all writer
         for writer_handle in writer_list:
            writer_handle.start()
            
         # Wait for writer
         for writer_handle in writer_list:
            
            writer_handle.join( TIMEOUT )
            
            if writer_handle.exitcode == 0:
               logging.info( "Writer: '" + writer_handle.name + "' successfully returned!" )
            else:
               
               if writer_handle.is_alive():
               
                  logging.info( "Killing writer process due timeout: %s" % writer_handle.name )
                  writer_handle.terminate()
      
      # Check if reader should be used...
      if args.reader_count and args.reader_count > 0:
         
         reader_list = list()
      
         for i in range( args.reader_count ):
            
            reader_name   = "reader" + str( i )
            reader_handle = multiprocessing.Process( name=reader_name, target=read_from_file, args=( args.target_file, block_size, ) )
            reader_list.append( reader_handle )
         
         # Start all reader
         for reader_handle in reader_list:
            reader_handle.start()
         
         # Wait for reader
         for reader_handle in reader_list:
            
            reader_handle.join( TIMEOUT )
            
            if reader_handle.exitcode == 0:
               logging.info( "Reader: '" + reader_handle.name + "' successfully returned!" )
            else:
               
               if reader_handle.is_alive():
               
                  logging.info( "Killing reader process due timeout: %s" % reader_handle.name )
                  reader_handle.terminate()
         
      end_time = time.time()
      duration = int( end_time - start_time )
      
      logging.info( "END - It took: %s seconds." % duration )
   
   except Exception as e:
      
      logging.error( str( e ) )

if __name__ == '__main__':
   main()