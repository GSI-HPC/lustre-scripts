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
import MySQLdb
import sys
import os
import datetime, time
import commands
import os.path
import re
import random
import multiprocessing

from pid_control import PID_Control
from contextlib import closing
from ctypes import c_char


payload      = None
rest_payload = None


ERROR_UNHANDLED       = 1
ERROR_OST_SPECIFIC    = 2
ERROR_FILE_INCOMPLETE = 3
ERROR_FILE_EMPTY      = 4
ERROR_FILE_NOT_FOUND  = 5


class OSTPerfInfo:
   
   def __init__( self, read_timestamp, write_timestamp, ost, ip, size, read_throughput, write_throughput, read_duration, write_duration ):
      
      self.read_timestamp   = read_timestamp
      self.write_timestamp  = write_timestamp
      self.ost              = ost
      self.ip               = ip
      self.size             = size
      self.read_throughput  = read_throughput
      self.write_throughput = write_throughput
      self.read_duration    = read_duration
      self.write_duration   = write_duration


class AutoRemoveFile:
   
   def __init__( self, filepath ):
      self.filepath = filepath
   
   def __enter__( self ):
      return self
   
   # TODO Guarantee cleanup in any case, even without using the with statement on object creation...
   def __exit__( self, exc_type, exc_value, traceback ):
      
      if os.path.exists( self.filepath ):
         os.remove( self.filepath )


def create_ost_perf_table( HOST, USER, PASSWD, DATABASE, TABLENAME ):
   
   with closing( MySQLdb.connect( host=HOST, user=USER, passwd=PASSWD, db=DATABASE ) )  as conn:
      with closing( conn.cursor() ) as cur:
         
         sql = """
CREATE TABLE """ + TABLENAME + """ (
   id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
   read_timestamp  TIMESTAMP NOT NULL DEFAULT "0000-00-00 00:00:00",
   write_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   ost CHAR(7) NOT NULL,
   ip CHAR(15) NOT NULL,
   size BIGINT(20) UNSIGNED NOT NULL,
   read_throughput BIGINT(20) UNSIGNED NOT NULL,
   write_throughput BIGINT(20) UNSIGNED NOT NULL,
   read_duration INT(10) UNSIGNED NOT NULL,
   write_duration INT(10) UNSIGNED NOT NULL,
   PRIMARY KEY (id)
) ENGINE=MyISAM DEFAULT CHARSET=latin1
"""

         cur.execute( sql )
         logging.debug( "Created table:\n" + sql )


def fill_ost_perf_table( HOST, USER, PASSWD, DATABASE, TABLENAME, ost_perf_info_dict ):
   
   if ost_perf_info_dict:
   
      sql = "INSERT INTO " + TABLENAME + "(read_timestamp, write_timestamp, ost, ip, size, read_throughput, write_throughput, read_duration, write_duration) VALUES "
      
      ost_perf_info_keys = ost_perf_info_dict.keys()
      
      num_ost_names = len( ost_perf_info_keys )
      
      if num_ost_names > 0:
         
         ost_perf_info_item = ost_perf_info_dict[ ost_perf_info_keys[ 0 ] ]
         
         sql += "('" + ost_perf_info_item.read_timestamp + "','" + ost_perf_info_item.write_timestamp + "','" + ost_perf_info_item.ost + "','" + ost_perf_info_item.ip + "'," + str( ost_perf_info_item.size ) + "," + str( int( round(  ost_perf_info_item.read_throughput, 0 ) ) ) + "," + str( int( round(  ost_perf_info_item.write_throughput, 0 ) ) ) + "," + str( int( round( ost_perf_info_item.read_duration, 0 ) ) ) + "," + str( int( round( ost_perf_info_item.write_duration, 0 ) ) ) + ")"
         
         if num_ost_names > 1:
            
            for i in range( 1, num_ost_names ):
               
               ost_perf_info_item = ost_perf_info_dict[ ost_perf_info_keys[ i ] ]
               
               sql += ",('" + ost_perf_info_item.read_timestamp + "','" + ost_perf_info_item.write_timestamp + "','" + ost_perf_info_item.ost + "','" + ost_perf_info_item.ip + "'," + str( ost_perf_info_item.size ) + "," + str( int( round(  ost_perf_info_item.read_throughput, 0 ) ) ) + "," + str( int( round(  ost_perf_info_item.write_throughput, 0 ) ) ) + "," + str( int( round( ost_perf_info_item.read_duration, 0 ) ) ) + "," + str( int( round( ost_perf_info_item.write_duration, 0 ) ) ) + ")"
         
         with closing( MySQLdb.connect( host=HOST, user=USER, passwd=PASSWD, db=DATABASE ) )  as conn:
            with closing( conn.cursor() ) as cur:
               
               logging.debug( "Executing SQL statement:\n" + sql )
               cur.execute( sql )
               
               if cur.rowcount != num_ost_names:
                  raise RuntimeError( "Number of rows inserted is not equal to number of input records for table: %s" % TABLENAME )
               
               logging.info( "Inserted %s records into %s" % ( str( num_ost_names ), TABLENAME ) )


def get_ost_state_lists( lfs_bin, ost_re_pattern ):

   active_ost_list   = list()
   inactive_ost_list = list()

   if not os.path.isfile( lfs_bin ):
      raise RuntimeError( "LFS binary was not found under: %s" % lfs_bin )
      
   cmd = lfs_bin + " check osts"
   
   ( status, output ) = commands.getstatusoutput( cmd )

   if status > 0:
      raise RuntimeError( "Error occurred during check on OSTs: %s" % output )

   if not output:
      raise RuntimeError( "Check OSTs returned an empty result!" )
   
   ost_list = output.split( '\n' )
   
   for ost_info in ost_list:
      
      idx_ost_name = ost_info.find( 'OST' )
      
      if idx_ost_name == -1:
         raise RuntimeError( "No OST name found in output line: %s" % ost_info )
      
      ost_name = ost_info[ idx_ost_name : idx_ost_name + 7 ]
      re_match = ost_re_pattern.match( ost_name )
      
      if not re_match:
         raise RuntimeError( "No valid OST name found in line: %s" % ost_info )
      
      if 'active' in ost_info:
         logging.debug( "Found active OST: %s" % ost_name )
         active_ost_list.append( ost_name )
      
      else:
         logging.debug( "Found inactive OST: %s" % ost_name )
         inactive_ost_list.append( ost_name )
   
   return ( active_ost_list, inactive_ost_list )


def get_ost_ip_dict( lctl_bin, ost_re_pattern, ip_re_pattern ):
   
   ost_ip_dict = dict()
   
   if not os.path.isfile( lctl_bin ):
      raise RuntimeError( "LCTL binary was not found under: %s" % lctl_bin )
   
   cmd = lctl_bin + " get_param 'osc.*.ost_conn_uuid'"
   
   ( status, output ) = commands.getstatusoutput( cmd )
   
   if status > 0:
      raise RuntimeError( "Error occurred during read of OST connection UUID information: %s" % output )
   
   if not output:
      raise RuntimeError( "OST connection UUID information read returned an empty result!" )

   ost_list = output.split( '\n' )
   
   for ost_info in ost_list:
      
      idx_ost_name = ost_info.find( 'OST' )
      
      if idx_ost_name == -1:
         raise RuntimeError( "No OST name found in output line: %s" % ost_info )

      idx_ost_name_term = ost_info.find( '-', idx_ost_name )

      if idx_ost_name_term == -1:
         raise RuntimeError( "Could not find end of OST name identified by '-' in: %s" % ost_info )

      ost_name = ost_info[ idx_ost_name : idx_ost_name_term ]
      
      re_match = ost_re_pattern.match( ost_name )
      
      if not re_match:
         raise RuntimeError( "No valid OST name found in output line: %s" % ost_info )
      
      ost_conn_uuid_str = 'ost_conn_uuid='
      
      idx_ost_conn_uuid = ost_info.find( ost_conn_uuid_str )
      
      if idx_ost_conn_uuid == -1:
         raise RuntimeError( "Could not find '%s' in line: %s" % ( ost_conn_uuid_str, ost_info ) )
      
      idx_ost_conn_uuid_term = ost_info.find( '@', idx_ost_conn_uuid )
      
      if idx_ost_conn_uuid_term == -1:
         raise RuntimeError( "Could not find terminating '@' for ost_conn_uuid identification: %s" % ost_info )
      
      ost_conn_ip = ost_info[ idx_ost_conn_uuid + len( ost_conn_uuid_str ) : idx_ost_conn_uuid_term ]
      
      ost_ip_dict[ ost_name ] = ost_conn_ip
   
   return ost_ip_dict


def get_ost_ip_mapping( ost_list, ost_ip_dict ):
   
   if not ost_list or len( ost_list ) == 0:
      raise RuntimeError( "OST list is empty!" )
   
   if not ost_ip_dict or len( ost_ip_dict ) == 0:
      raise RuntimeError( "OST IP dict is empty!" )
   
   ost_ip_mapping_dict = dict()
   
   for ost_name in ost_list:
      
      if ost_name in ost_ip_dict:
         ost_ip_mapping_dict[ ost_name ] = ost_ip_dict[ ost_name ]
      else:
         raise RuntimeError( "No IP could be found for the OST: %s" % ost_name )
   
   return ost_ip_mapping_dict


def set_stripe( lfs_bin, ost_name, filepath ):
   
   if not os.path.isfile( lfs_bin ):
      raise RuntimeError( "LFS binary was not found under: %s" % lfs_bin )
   
   # TODO Class variable
   ost_prefix_len = len( 'OST' )
   
   ost_idx = ost_name[ ost_prefix_len : ]
   
   # No striping over multiple OSTs
   cmd = lfs_bin + " setstripe --stripe-index 0x" + ost_idx + " --stripe-count 1 --stripe-size 0 " + filepath
   
   ( status, output ) = commands.getstatusoutput( cmd )
   
   if status > 0:
      raise RuntimeError( "Failed to set stripe for file: %s\n%s" % ( filepath, output )  )
   
   if not os.path.isfile( filepath ):
      raise RuntimeError( "Failed to create file via setstripe under: %s" % filepath )


def initialize_payload( total_size, block_size ):
   
   global payload
   
   payload = multiprocessing.RawArray( c_char, block_size )
   
   start_time = time.time() * 1000.0
   
   # TODO Create random numbers...
   # No random numbers since no compression is used in Lustre FS directly.
   payload.value = "".join( 'A' for i in xrange( block_size ) )
   
   end_time = time.time() * 1000.0
   duration = round( ( end_time - start_time ) / 1000.0, 2 )
   logging.debug( "Creating payload took: %s seconds." % str( duration ) )
   
   rest_payload_size = total_size % block_size
   
   if rest_payload_size > 0:
      
      global rest_payload
      
      rest_payload = multiprocessing.RawArray( c_char, rest_payload_size )
      
      start_time = time.time() * 1000.0
      
      rest_payload.value = "".join( 'A' for i in xrange( rest_payload_size ) )
   
      end_time = time.time() * 1000.0
      duration = round( ( end_time - start_time ) / 1000.0, 2 )
      logging.debug( "Creating rest payload took: %s seconds." % str( duration ) )


def write_global_data( filepath, block_bytes, total_bytes ):
   
   iterations = total_bytes / block_bytes
   
   start_time = time.time() * 1000.0
   
   with open( filepath, 'w' ) as f:
      
      for i in xrange( int ( iterations ) ):
         f.write( payload.raw )
      
      if rest_payload:
         f.write( rest_payload.raw )
   
   end_time = time.time() * 1000.0
   duration = ( end_time - start_time ) / 1000.0
   
   return duration


def read_data( filepath, block_size, total_size ):
   
   total_read_bytes = 0
   
   start_time = time.time() * 1000.0
   
   with open( filepath ) as f:
      
      read_bytes        = f.read( block_size )
      total_read_bytes += len( read_bytes )
      
      while read_bytes:
         
         read_bytes        = f.read( block_size )
         total_read_bytes += len( read_bytes )
   
   end_time = time.time() * 1000.0
   duration = ( end_time - start_time ) / 1000.0
   
   if total_read_bytes != total_size:
      raise RuntimeError( "Read bytes differ from total size!" )
   
   return duration


def writer_func( lfs_bin, filepath, ost_name, block_size, total_size, result_queue ):
   
   try:
      
      if os.path.exists( filepath ):
         os.remove( filepath )
      
      logging.debug( "Setting stripe for file: %s on OST: %s" % ( filepath, ost_name ) )
      
      set_stripe( lfs_bin, ost_name, filepath )
      
      logging.debug( "Writing output file to: %s" % filepath )
      
      timestamp = datetime.datetime.fromtimestamp( time.time() ).strftime( '%Y-%m-%d %H:%M:%S' )
      
      duration = write_global_data( filepath, block_size, total_size )
      
      file_size = os.path.getsize( filepath )
      
      if file_size == total_size:
         
         result_queue.put( [ timestamp, duration ] )
         exit( 0 )
      
      elif file_size == 0:
         
         logging.error( "File is empty: %s" % filepath )
         exit( ERROR_FILE_EMPTY )
      
      else:
         
         logging.error( "File is incomplete: %s" % filepath )
         exit( ERROR_FILE_INCOMPLETE )
   
   except Exception as e:
      
      error_msg = str( e )
      
      logging.error( "Catched exception in writer for %s: %s" % ( ost_name, error_msg ) )
      
      if 'OST' in error_msg:
         
         logging.error( "Found OST specific error..." )
         exit( ERROR_OST_SPECIFIC )
      
      logging.error( "Caught unhandled error..." )
      exit( ERROR_UNHANDLED )


def reader_func( filepath, ost_name, block_size, total_size, result_queue ):
   
   try:
      
      with AutoRemoveFile( filepath ) as auto_remove_file:
         
         if os.path.exists( filepath ):
            
            file_size = os.path.getsize( filepath )
            
            if file_size == total_size:
            
               logging.debug( "Reading output file from: %s" % filepath )
               
               timestamp = datetime.datetime.fromtimestamp( time.time() ).strftime( '%Y-%m-%d %H:%M:%S' )
               duration  = read_data( filepath, block_size, total_size )
               
               result_queue.put( [ timestamp, duration ] )
               exit( 0 )
            
            elif file_size == 0:
               
               logging.error( "File is empty: %s" % filepath )
               exit( ERROR_FILE_EMPTY )
            
            else:
               
               logging.error( "File is incomplete: %s" % filepath )
               exit( ERROR_FILE_INCOMPLETE )
            
         else:
            logging.error( "No file to be read could be found under: %s" % filepath )
            exit( ERROR_FILE_NOT_FOUND )
   
   except Exception as e:
      
      error_msg = str( e )
      
      logging.error( "Catched exception in reader for %s: %s" % ( ost_name, error_msg ) )
      
      if 'OST' in error_msg:
         
         logging.error( "Found OST specific error..." )
         exit( ERROR_OST_SPECIFIC )
      
      logging.error( "Caught unhandled error..." )
      exit( ERROR_UNHANDLED )


def main():

   start_time = time.time() * 1000.0

   parser = argparse.ArgumentParser( description='Tests simply Lustre OST read and write performance to check availability and detect noticeable problems.' )
   parser.add_argument('-f', '--config-file', dest='config_file', type=str, required=True, help='Path to the config file.')
   parser.add_argument( '-D', '--enable-debug', dest='enable_debug', required=False, action='store_true', help='Enables debug log messages.' )
   parser.add_argument( '--create-table', dest='create_table', required=False, action='store_true', help='Creates database specific table.' )
   
   args = parser.parse_args()
   
   if not os.path.isfile( args.config_file ):
      raise IOError( "The config file does not exist or is not a file: %s" % args.config_file )
   
   config = ConfigParser.ConfigParser()
   config.read( args.config_file )
   
   log_filename = config.get( 'logging', 'filename' )
   
   log_level = logging.INFO
   
   if args.enable_debug:
      log_level = logging.DEBUG

   logging.basicConfig( filename=log_filename, level=log_level, format="%(asctime)s - %(levelname)s: %(message)s" )
   
   PID_LOC_DIR = config.get( 'control', 'pid_loc_dir' )
   OST_RE_PATTERN = re.compile( config.get( 'control', 'ost_reg_ex' ) )
   IP_RE_PATTERN  = re.compile( config.get( 'control', 'ip_reg_ex' ) )
   
   HOST     = config.get( 'mysqld', 'host' )
   USER     = config.get( 'mysqld', 'user' )
   PASSWD   = config.get( 'mysqld', 'password' )
   DATABASE = config.get( 'mysqld', 'database' )
   
   TABLENAME = config.get( 'database', 'table' )
   
   LCTL_BIN = config.get( 'lustre', 'lctl_bin' )
   LFS_BIN  = config.get( 'lustre', 'lfs_bin' )
   LFS_PATH = config.get( 'lustre', 'lfs_path' )
   
   OST_PERF_TEST_DIR = config.get( 'test', 'lfs_target_dir' )
   FILENAME_POSTFIX  = config.get( 'test', 'filename_postfix' )
   ENABLE_READ       = bool( config.get( 'test', 'enable_read' ) )
   BLOCK_SIZE        = int( config.get( 'test', 'block_size' ) )
   TOTAL_SIZE        = int( config.get( 'test', 'total_size' ) )
   TIMEOUT           = int( config.get( 'test', 'timeout' ) )
   
   try:
      
      pid_file = PID_LOC_DIR + os.path.sep + os.path.basename( sys.argv[0] ) + ".pid"
      
      logging.debug( "PID file location: %s" % pid_file )
      
      with PID_Control( pid_file ) as pid_control:
         
         if pid_control.lock():
            
            logging.info( 'START' )
            
            if not LFS_PATH in OST_PERF_TEST_DIR:
               raise RuntimeError( "Check set paths in the configuration file!" )
      
            if args.create_table:
               create_ost_perf_table( HOST, USER, PASSWD, DATABASE, TABLENAME )
            
            ost_perf_info_dict = dict()
            
            ost_ip_dict = get_ost_ip_dict( LCTL_BIN, OST_RE_PATTERN, IP_RE_PATTERN )
            
            active_ost_list, inactive_ost_list = get_ost_state_lists( LFS_BIN, OST_RE_PATTERN )
            
            if active_ost_list:
            
               active_ost_ip_dict = get_ost_ip_mapping( active_ost_list, ost_ip_dict )
            
               if active_ost_ip_dict:
                  
                  initialize_payload( TOTAL_SIZE, BLOCK_SIZE )
               
                  # Testing write performance
                  for ost_name in active_ost_ip_dict:
                     
                     try:
                     
                        filepath = OST_PERF_TEST_DIR + os.path.sep + ost_name + FILENAME_POSTFIX
                        
                        collectable = False
                        timestamp   = datetime.datetime.fromtimestamp( time.time() ).strftime( '%Y-%m-%d %H:%M:%S' )
                        duration    = 0
                        throughput  = 0
                        
                        result_queue  = multiprocessing.Queue()
                        writer_handle = multiprocessing.Process( name=str( "writer_" + ost_name ), target=writer_func, args=( LFS_BIN, filepath, ost_name, BLOCK_SIZE, TOTAL_SIZE, result_queue ) )
                        
                        writer_handle.start()
                        writer_handle.join( TIMEOUT )
                        
                        if writer_handle.exitcode == 0 and not result_queue.empty():
                           
                           result = result_queue.get()
                           
                           if result:
                              
                              timestamp = result[0]
                              duration  = result[1]
                              
                              if duration:
                                 throughput = TOTAL_SIZE / duration
                              else:
                                 throughput = 0
                           
                              collectable = True
                        
                        elif writer_handle.is_alive():
                        
                           logging.info( "Killing writer due timeout: %s" % writer_handle.name )
                              
                           writer_handle.terminate()
                              
                           duration = TIMEOUT
                              
                           collectable = True
                        
                        elif writer_handle.exitcode == ERROR_OST_SPECIFIC:
                           collectable = True
                        
                        elif writer_handle.exitcode == ERROR_FILE_INCOMPLETE:
                           collectable = True
                        
                        elif writer_handle.exitcode == ERROR_FILE_EMPTY:
                           collectable = True
                        
                        else:
                           logging.warning( "No action after write test for: %s" % writer_handle.name )
                        
                        if collectable:
                           
                           ost_ip = active_ost_ip_dict[ ost_name ]
                           
                           logging.debug( "[WRITE-TEST] Timestamp: %s, OST: %s, IP: %s, Payload: %s, Throughput: %s, Duration: %s" % ( timestamp, ost_name, ost_ip, TOTAL_SIZE, throughput, duration ) )
                           
                           ost_perf_info_dict[ ost_name ] = OSTPerfInfo( '0000-00-00 00:00:00', timestamp, ost_name, ost_ip, TOTAL_SIZE, 0, throughput, 0, duration )
                        
                        else:
                           logging.warning( "Not collectable for %s." % ost_name )
                  
                     except Exception as e:
                        logging.error( "Caught exception during OST write test: " + str( e ) )
                  
                  
                  if ENABLE_READ:
                     
                     for ost_name in ost_perf_info_dict:
                     
                        try:
                           
                           filepath = OST_PERF_TEST_DIR + os.path.sep + ost_name + FILENAME_POSTFIX
                           
                           ost_perf_info_item = ost_perf_info_dict[ ost_name ]
                           
                           if ost_perf_info_item:
                              
                              collectable = False
                              timestamp   = datetime.datetime.fromtimestamp( time.time() ).strftime( '%Y-%m-%d %H:%M:%S' )
                              duration    = 0
                              throughput  = 0
                              
                              ost_perf_info_item.read_timestamp  = timestamp
                              
                              result_queue  = multiprocessing.Queue()
                              reader_handle = multiprocessing.Process( name=str( "reader_" + ost_name ), target=reader_func, args=( filepath, ost_name, BLOCK_SIZE, TOTAL_SIZE, result_queue ) )
                              
                              reader_handle.start()
                              reader_handle.join( TIMEOUT )
                              
                              if reader_handle.exitcode == 0 and not result_queue.empty():
                                 
                                 result = result_queue.get()
                              
                                 if result:
                                    
                                    timestamp = result[0]
                                    duration  = result[1]
                                    
                                    if duration:
                                       throughput = TOTAL_SIZE / duration
                                    else:
                                       throughput = 0
                                    
                                    collectable = True
                              
                              elif reader_handle.is_alive():
                              
                                 logging.info( "Killing reader due timeout: %s" % reader_handle.name )
                                 
                                 reader_handle.terminate()
                                 
                                 duration = TIMEOUT
                                 
                                 collectable = True
                              
                              if collectable:
                              
                                 ost_ip = active_ost_ip_dict[ ost_name ]
                                 
                                 logging.debug( "[READ-TEST] Timestamp: %s, OST: %s, IP: %s, Payload: %s, Throughput: %s, Duration: %s" % ( timestamp, ost_name, ost_ip, TOTAL_SIZE, throughput, duration ) )
                                 
                                 ost_perf_info_item = ost_perf_info_dict[ ost_name ]
                                 
                                 ost_perf_info_item.read_timestamp  = timestamp
                                 ost_perf_info_item.read_duration   = duration
                                 ost_perf_info_item.read_throughput = throughput
                           
                           else:
                              logging.error( "No proper OSTPerfInfo item found for: %s" % ost_name )
                        
                        except Exception as e:
                           logging.error( "Caught  exception during OST write test: " + str( e ) )
            
            if inactive_ost_list:
               
               inactive_ost_ip_dict = get_ost_ip_mapping( inactive_ost_list, ost_ip_dict )
               
               if inactive_ost_ip_dict:
                  
                  timestamp = datetime.datetime.fromtimestamp( time.time() ).strftime( '%Y-%m-%d %H:%M:%S' )
                  
                  for ost_name in inactive_ost_ip_dict:
                  
                     ost_ip = inactive_ost_ip_dict[ ost_name ]
                     
                     ost_perf_info_dict[ ost_name ] = OSTPerfInfo( timestamp, timestamp, ost_name, ost_ip, TOTAL_SIZE, 0, 0, 0, 0 )
            
            if len( ost_perf_info_dict ):
               
               logging.debug( "Filling database with test results..." )
               
               fill_ost_perf_table( HOST, USER, PASSWD, DATABASE, TABLENAME, ost_perf_info_dict )
               
            else:
               raise RuntimeError( "No data collected for inserting into database!" )
         
         else:
            logging.warning( 'Skipped execution... Another instance is probably already running!' )
            return 0
   
   except Exception as e:
      logging.error( "Catch exception on last instance: " + str( e ) )
   
   end_time = time.time() * 1000.0
   duration = round( ( end_time - start_time ) / 1000.0, 2 )
   
   logging.info( "END - It took: %s seconds." % duration )

if __name__ == '__main__':
   main()