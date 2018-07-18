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
import re
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

from lib.disk_usage_info import lustre_ost_disk_usage_info_decimal_base_2




DATE_REG_EXP=r'^\d{4}-\d{2}-\d{2}$'


def raise_option_not_found( section, option ):
   raise Exception( "Option: " + option + " was not found in section: " + section )


def validate_config_file( config ):
   
   if not config.has_section( 'mysqld' ):
      raise ConfigParser.NoSectionError( 'Section mysqld was not found!' )

   if not config.has_option( 'mysqld', 'host' ):
      raise_option_not_found( 'mysqld', 'host' )

   if not config.has_option( 'mysqld', 'user' ):
      raise_option_not_found( 'mysqld', 'user' )
   
   if not config.has_option( 'mysqld', 'password' ):
      raise_option_not_found( 'mysqld', 'password' )
   
   if not config.has_section( 'robinhood' ):
      raise ConfigParser.NoSectionError( 'Section robinhood was not found!' )
   
   if not config.has_option( 'robinhood', 'database' ):
      raise_option_not_found( 'robinhood', 'database' )
   
   if not config.has_option( 'robinhood', 'acct_stat_table' ):
      raise_option_not_found( 'robinhood', 'acct_stat_table' )
   
   if not config.has_section( 'history' ):
      raise ConfigParser.NoSectionError( 'Section history was not found!' )
   
   if not config.has_option( 'history', 'database' ):
      raise_option_not_found( 'history', 'database' )
   
   if not config.has_option( 'history', 'acct_stat_history_table' ):
      raise_option_not_found( 'history', 'acct_stat_history_table' )
   
   if not config.has_option( 'chart', 'num_top_groups' ):
      raise_option_not_found( 'chart', 'num_top_groups' )
   
   if not config.has_option( 'chart', 'save_dir' ):
      raise_option_not_found( 'chart', 'save_dir' )
   
   if not config.has_option( 'chart_pie', 'filename' ):
      raise_option_not_found( 'chart_pie', 'filename' )
   
   if not config.has_option( 'chart_pie', 'filetype' ):
      raise_option_not_found( 'chart_pie', 'filetype' )
   
   if not config.has_section( 'mail' ):
      raise ConfigParser.NoSectionError( 'Section mail was not found!' )
   
   if not config.has_option( 'mail', 'server' ):
      raise_option_not_found( 'mail', 'server' )
   
   if not config.has_option( 'mail', 'sender' ):
      raise_option_not_found( 'mail', 'sender' )
   
   if not config.has_option( 'mail', 'recipient_list' ):
      raise_option_not_found( 'mail', 'recipient_list' )
   
   if not config.has_option( 'storage', 'filesystem' ):
      raise_option_not_found( 'storage', 'filesystem' )


def send_mail( server, subject, sender, receiver, text, filepath ):
   
   msg = MIMEMultipart()
   msg['Subject'] = subject
   msg['From']    = sender
   msg['To']      = ', '.join( receiver )
   
   msg.attach( MIMEText( text ) )
   
   filename = os.path.split( filepath )[1]
   filetype = filename.split( '.' )[1]
   
   img = MIMEImage( file( filepath ).read(), _subtype=filetype )
   img.add_header( 'Content-Disposition', "attachment; filename= %s" % filename )
   
   msg.attach( img )
   
   mail_server = smtplib.SMTP( server )
   mail_server.sendmail( sender, receiver, msg.as_string() )
   
   mail_server.quit()
   
   

def create_mail( sender, subject, receiver, text ):
   
   mail = """From: <""" + sender + """> 
To: <""" + receiver + """>
Subject: """ + subject + """ 

""" + text
   
   return mail


PB_DIVISIOR = Decimal( 1125899906842624.0 )
TB_DIVISIOR = Decimal( 1099511627776.0 )
GB_DIVISIOR = Decimal( 1073741824.0 )
MB_DIVISIOR = Decimal( 1048576.0 )
KB_DIVISIOR = Decimal( 1024.0 )
B_DIVISIOR  = Decimal( 1.0 )

def format_number_to_base_2_byte_unit( number ):
   
   if not isinstance( number, Number ):
      raise TypeError( "Provided value is not a number: %s" % str( number ) )
   
   result = None
   
   if number >= PB_DIVISIOR:
      result = Decimal( number ) / PB_DIVISIOR
      result = round( result, 2 )
      result = str( result ) + "PiB"
   
   elif number >= TB_DIVISIOR:
      result = number / TB_DIVISIOR
      result = round( result, 2 )
      result = str( result ) + "TiB"
   
   elif number >= GB_DIVISIOR:
      result = number / GB_DIVISIOR
      result = round( result, 2 )
      result = str( result ) + "GiB"
   
   elif number >= MB_DIVISIOR:
      result = number / MB_DIVISIOR
      result = round( result, 2 )
      result = str( result ) + "MiB"
   
   elif number >= KB_DIVISIOR:
      result = number / KB_DIVISIOR
      result = round( result, 2 )
      result = str( result ) + "KiB"
   
   elif number >= B_DIVISIOR:
      result = number / B_DIVISIOR
      result = str( result ) + "B"
   
   else:
      raise ValueError( "Failed to format number to a supported byte unit: %s" % str( number ) )
   
   return result


def validate_snapshot_date( snapshot_date ):

   if snapshot_date:
      
      reg_exp_match = re.match( DATE_REG_EXP, snapshot_date )
      
      if not reg_exp_match:
         raise RuntimeError( "No valid date format in snapshot date found: %s" % date )


def get_total_size_from_history_table( cur, history_acct_table, snapshot_date ):
   
   sql = "SELECT SUM(size) FROM " + history_acct_table + " WHERE date='" + snapshot_date + "'"
   logging.debug( sql )
   cur.execute( sql )
   
   total_size = cur.fetchone()[0]
   
   if not cur.rowcount or not total_size:
      raise RuntimeError( "Failed to retrieve total sum for size from accounting history table for date: " + snapshot_date )
   
   return total_size


def get_top_group_list_from_history_table( cur, history_acct_table, snapshot_date, num_top_groups ):
   
   sql = "SELECT gid, SUM(size) as group_size FROM " + history_acct_table + " WHERE date='" + snapshot_date + "' GROUP BY gid ORDER BY group_size DESC LIMIT " + num_top_groups
   logging.debug( sql )
   cur.execute( sql )
   
   group_by_size_list = cur.fetchall()
   group_info_list    = list()
   
   if not cur.rowcount or not group_by_size_list:
      raise RuntimeError( "Failed to retrieve group by size list from accounting history table for date: " + snapshot_date )
   
   for group_entry in group_by_size_list:
      group_info_list.append( GroupInfo( group_entry[0], group_entry[1], 0 ) )
   
   return group_info_list


def get_total_size( cur, acct_table ):
   
   sql = "SELECT SUM(size) FROM " + acct_table + " WHERE type='file'"
   logging.debug( sql )
   cur.execute( sql )
   
   total_size = cur.fetchone()[0]
   
   if not cur.rowcount or not total_size:
      raise RuntimeError( 'Failed to retrieve total sum for size from accounting table!' )
   
   return total_size


def get_top_group_list( cur, acct_table, num_top_groups ):
   
   sql = "SELECT gid, SUM(size) as group_size FROM " + acct_table + " WHERE type='file' GROUP BY gid ORDER BY group_size DESC LIMIT " + num_top_groups
   logging.debug( sql )
   cur.execute( sql )
   
   group_by_size_list = cur.fetchall()
   group_info_list    = list()
   
   if not cur.rowcount or not group_by_size_list:
      raise RuntimeError( 'Failed to retrieve group by size list from accounting table!' )
   
   for group_entry in group_by_size_list:
      group_info_list.append( GroupInfo( group_entry[0], group_entry[1], 0 ) )
   
   return group_info_list


def calc_others_size( group_info_list, total_size ):
   
   group_size = 0
   
   for group_info in group_info_list:
      group_size += group_info.size
   
   logging.debug( "Total size of aggregated groups: %s" % group_size )
   
   return total_size - group_size


def create_chart_pie( title, group_info_list, others_size, used_total_size, ost_total_size, snapshot_timestamp, chart_pie_path ):
   
   filepath = chart_pie_path
   filetype = os.path.split( chart_pie_path )[1].split('.')[1]
   
   labels = []
   sizes  = []
   
   for group_info in group_info_list:
      
      label_text = group_info.gid + " (" + format_number_to_base_2_byte_unit( group_info.size ) + ")"
      
      labels.append( label_text )
      sizes.append( group_info.size )
   
   labels.append( "others (" + format_number_to_base_2_byte_unit( others_size ) + ")" )
   sizes.append( others_size )
   
   creation_timestamp_text = "Timestamp: " + snapshot_timestamp
   
   fig, ax = plt.subplots()
   
   fig.suptitle( title, fontsize = 18, fontweight = 'bold' )  
   fig.subplots_adjust( top=0.80 )
   
   cs_range = float( len( sizes ) ) * 1.1
   colors = cm.Set1( plt.np.arange( cs_range ) / cs_range )
   
   patches, texts, autotexts = ax.pie( sizes, labels = labels, colors = colors, autopct = '%1.2f%%', pctdistance = .8, shadow = False, startangle = 90 )
   ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
   
   for autotext_item in autotexts:
      autotext_item.set_fontsize( 10 )
   
   pct_used_total_size = int( ( used_total_size / ost_total_size ) * Decimal( 100 ) )
   
   size_info = "Used " + format_number_to_base_2_byte_unit( used_total_size ) + " of " + format_number_to_base_2_byte_unit( ost_total_size ) + " Volume (" + str( pct_used_total_size ) + "%)"
   
   ax.set_title( size_info, y = 1.125, fontsize = 14 )
   
   ax.text( 0, 0, creation_timestamp_text, fontsize = 8, verticalalignment = 'bottom', horizontalalignment = 'left', transform = fig.transFigure )
   
   fig.set_size_inches( 10, 8 )
   
   fig.savefig( filepath, format=filetype, dpi=200 )
   
   logging.debug( "Saved created pie chart under: %s" % chart_pie_path )


def cleanup_files( dir_path, pattern ):
   
   if not os.path.isdir( dir_path ):
      raise RuntimeError( "Directory does not exist under: %s" % dir_path )
   
   file_list = os.listdir( dir_path )
   
   for filename in file_list:
      
      if pattern in filename:
         
         file_path = os.path.join( dir_path, filename )
         
         os.remove( file_path )
         
         logging.debug( "Removed file during cleanup procedure: %s" % file_path )


class GroupInfo:
   
   def __init__( self, gid, size, count ):
      
      self.gid   = gid
      self.size  = size
      self.count = count


def main():

   parser = argparse.ArgumentParser( description='Creates report.' )
   parser.add_argument( '-f', '--config-file', dest='config_file', type=str, required=True, help='Path of the config file.' )
   parser.add_argument( '-D', '--enable-debug', dest='enable_debug', required=False, action='store_true', help='Enables logging of debug messages.' )
   parser.add_argument( '--snapshot-date', dest='snapshot_date', required=False, type=str, help="Specifies snapshot date for creating pie chart from history table in format: Y-m-d" )
   parser.add_argument( '--no-mail', dest='no_mail', required=False, action='store_true', help="Disables e-mail notification." )
   
   args = parser.parse_args()
   
   if not os.path.isfile( args.config_file ):
      raise IOError( "The config file does not exist or is not a file: " + args.config_file )
   
   logging_level = logging.INFO
   
   if args.enable_debug:
      logging_level = logging.DEBUG

   logging.basicConfig( level=logging_level, format='%(asctime)s - %(levelname)s: %(message)s' )

   logging.info( 'START' )
   
   config = ConfigParser.ConfigParser()
   config.read( args.config_file )
   
   validate_config_file( config )
   
   database           = None
   acct_stat_table    = None
   snapshot_date      = None
   snapshot_timestamp = None
   
   num_top_groups   = config.get( 'chart', 'num_top_groups' )
   chart_save_dir   = config.get( 'chart', 'save_dir' )
   
   chart_pie_filename = config.get( 'chart_pie', 'filename' )
   chart_pie_filetype = config.get( 'chart_pie', 'filetype' )
   
   filesystem = config.get( 'storage', 'filesystem' )
   
   logging.debug( "Number of top group: %s" % num_top_groups )
   
   try:
      
      ost_total_size = lustre_ost_disk_usage_info_decimal_base_2( filesystem )
      
      if not os.path.isdir( chart_save_dir ):
         raise RuntimeError( "Directory does not exist for saving charts: %s" % chart_save_dir )
      
      if args.snapshot_date:
         
         validate_snapshot_date( args.snapshot_date )
         
         database        = config.get( 'history', 'database' )
         acct_stat_table = config.get( 'history', 'acct_stat_history_table' )
         snapshot_date   = args.snapshot_date
         
         snapshot_timestamp   = snapshot_date + ' - 23:59:59'
      
      else:
         
         database        = config.get( 'robinhood', 'database' )
         acct_stat_table = config.get( 'robinhood', 'acct_stat_table' )
         
         now                = datetime.datetime.now()
         snapshot_date      = now.strftime( '%Y-%m-%d' )
         snapshot_timestamp = snapshot_date + " - " + now.strftime( '%X' )
      
      cleanup_files( chart_save_dir, chart_pie_filename )
      
      logging.debug( "Report date: %s" % snapshot_date )
      
      title           = "Storage Report of " + filesystem 
      used_total_size = 0
      others_size     = 0
      group_info_list = None
      
      chart_pie_path = os.path.abspath( chart_save_dir + os.path.sep + chart_pie_filename + "_" + snapshot_date + "." + chart_pie_filetype )
      
      with closing( MySQLdb.connect( host=config.get( 'mysqld', 'host' ), user=config.get( 'mysqld', 'user' ), passwd=config.get( 'mysqld', 'password' ), db=database ) )  as conn:
         with closing( conn.cursor() ) as cur:
            
            if args.snapshot_date:
               
               used_total_size = get_total_size_from_history_table( cur, acct_stat_table, snapshot_date )
               
               group_info_list = get_top_group_list_from_history_table( cur, acct_stat_table, snapshot_date, num_top_groups )
            
            else:
         
               used_total_size = get_total_size( cur, acct_stat_table )
               
               group_info_list = get_top_group_list( cur, acct_stat_table, num_top_groups )
      
      
      
      others_size = calc_others_size( group_info_list, used_total_size )
      
      logging.debug( "Total size: %s" % used_total_size )
      logging.debug( "Other size: %s" % others_size )
      
      logging.debug( "File path for pie chart: %s" % chart_pie_path )
      
      create_chart_pie( title, group_info_list, others_size, used_total_size, ost_total_size, snapshot_timestamp, chart_pie_path )
      
      if ( not args.no_mail ) and ( os.path.isfile( chart_pie_path ) ):
         
         mail_server         = config.get( 'mail', 'server' )
         mail_sender         = config.get( 'mail', 'sender' )
         mail_recipient_list = config.get( 'mail', 'recipient_list' ).replace(' ', '').split( ',' )
         
         mail_subject = "Storage Report for " + filesystem
         mail_text    = "Please check the attached image file for the storage report of " + filesystem + "."
         
         send_mail( mail_server, mail_subject, mail_sender, mail_recipient_list, mail_text, chart_pie_path )
      
      logging.info( 'END' )
      
      return 0
   
   except Exception as e:
      
      error_msg = str( e )
      
      logging.error( error_msg )
      
      if ( not args.no_mail ):
         
         mail_server    = config.get( 'mail', 'server' )
         mail_sender    = config.get( 'mail', 'sender' )
         mail_recipient = config.get( 'mail', 'recipient_list' )
         
         mail_subject = __file__ + " - Error Occured!"
         mail_text    = error_msg
         
         mail = create_mail( mail_sender, mail_subject, mail_recipient, mail_text )
         
         smtp_conn = smtplib.SMTP( mail_server )
         smtp_conn.sendmail( mail_sender, mail_recipient, mail )
         
         smtp_conn.quit()
      
      logging.info( "Error notification mail has been sent to: " + mail_recipient )


if __name__ == '__main__':
   main()
