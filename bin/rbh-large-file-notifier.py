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
import time, datetime
import smtplib
import commands

from contextlib import closing
from cStringIO import StringIO
from subprocess import Popen

from lib.entries_table_handler  import EntriesTableHandler
from lib.notifier_table_handler import NotifierTableHandler, NotifyInfo


FILES_REG_EXP=r'^\d{1,3}(GB|TB)$'

# TODO MAIL VALIDATION VIA REG EX!!!!

GB_MULTIPLIER=1073741824
TB_MULTIPLIER=1099511627776


def validate_config_file( config ):
   
   if not config.has_section( 'mysqld' ):
      raise ConfigParser.NoSectionError( 'Section mysqld was not found!' )

   if not config.has_option( 'mysqld', 'host' ):
      raise ConfigParser.NoOptionError( 'Option host was not found in section mysqld!' )
   
   if not config.has_option( 'mysqld', 'database' ):
      raise ConfigParser.NoOptionError( 'Option database was not found in section mysqld!' )

   if not config.has_option( 'mysqld', 'user' ):
      raise ConfigParser.NoOptionError( 'Option user was not found in section mysqld!' )
   
   if not config.has_option( 'mysqld', 'password' ):
      raise ConfigParser.NoOptionError( 'Option password was not found in section mysqld!' )

   if not config.has_section( 'check' ):
      raise ConfigParser.NoSectionError( 'Section check was not found!' )

   if not config.has_option( 'check', 'file_size' ):
      raise ConfigParser.NoOptionError( 'Option file_size was not found in section check!' )
   
   if not config.has_option( 'check', 'file_system' ):
      raise ConfigParser.NoOptionError( 'Option file_system was not found in section check!' )
   
   if not config.has_option( 'check', 'check_interval_days' ):
      raise ConfigParser.NoOptionError( 'Option check_interval_days was not found in section check!' )
   
   if not config.has_option( 'notify', 'table' ):
      raise ConfigParser.NoOptionError( 'Option table was not found in section notify!' )
   
   if not config.has_option( 'notify', 'database' ):
      raise ConfigParser.NoOptionError( 'Option database was not found in section notify!' )
   
   if not config.has_option( 'mail', 'server' ):
      raise ConfigParser.NoOptionError( 'Option server was not found in section mail!' )
   
   if not config.has_option( 'mail', 'sender' ):
      raise ConfigParser.NoOptionError( 'Option sender was not found in section mail!' )
   
   if not config.has_option( 'mail', 'subject' ):
      raise ConfigParser.NoOptionError( 'Option subject was not found in section mail!' )
   
   if not config.has_option( 'mail', 'overview_recipient' ):
      raise ConfigParser.NoOptionError( 'Option overview_recipient was not found in section mail!' )
   
   if not config.has_option( 'mail', 'send_user_mail' ):
      raise ConfigParser.NoOptionError( 'Option send_user_mail was not found in section mail!' )
   
   if not config.has_option( 'ldap', 'server' ):
      raise ConfigParser.NoOptionError( 'Option server was not found in section ldap!' )
   
   if not config.has_option( 'ldap', 'dc' ):
      raise ConfigParser.NoOptionError( 'Option dc was not found in section ldap!' )
   
   reg_exp_match = re.match( FILES_REG_EXP, config.get( 'check', 'file_size' ) )
   
   if not reg_exp_match:
      raise RuntimeError( 'Failed validation of valid file size format!' )
   
   send_user_mail = config.get( 'mail', 'send_user_mail' )
   
   if not ( send_user_mail == 'off' or send_user_mail == 'on' ):
      raise RuntimeError( "Option send_user_mail is allowed only to be 'off' or 'on'!" )


def calc_threshold( file_size_spec ):
   
   if 'GB' in file_size_spec:
      return int ( file_size_spec.split( 'GB' )[0] ) * GB_MULTIPLIER
   
   if 'TB' in file_size_spec:
      return int ( file_size_spec.split( 'TB' )[0] ) * TB_MULTIPLIER
   
   raise RuntimeError( 'No file size threshold could be calculated!' )


def create_user_mail_body( uid, file_system, file_size, file_list ):
   
   text = """Dear user (uid=""" + uid + """),

this is an automated e-mail that contains a list of your stored files on '""" + file_system + """' that are equal or larger than """ + file_size + """.

Please check if you really need those files stored on the file system.

Due to high memory preasure in the ZFS backend file system,
large files can result in serious performance issues that may concern yourself and others directly.

If you do not need those files anymore, please delete it.
In case it is necessary to keep a large file stored on the file system, please write an e-mail to 'hpc-data@gsi.de' with a specific reason.

The following information is provided in CSV format: size;path\n\n""" + file_list + """

On behalf of the GSI HPC-Data group
"""
   
   return text


def create_mail( sender, subject, receiver, text ):
   
   mail = """From: <""" + sender + """>
To: <""" + receiver + """>
Subject: """ + subject + """ 

""" + text
   
   return mail


def uid_to_mail_ldap( ldap_server, ldap_dc, uid ):
   
   cmd = "KRB5CCNAME=/tmp/krb5cc_nslcd sudo -u nslcd ldapsearch -Y GSSAPI -H ldap://" + ldap_server + "/ -b ou=people,dc=" + ldap_dc + ",dc=de '(uid=" + uid + ")' mail"
   
   # TODO Deprecated!
   ( status, output ) = commands.getstatusoutput( cmd )
   
   # Python 3.1
   ##subprocess.getstatusoutput( cmd )
   
   if status > 0:
      
      logging.error( "ldapsearch failed to retrieve e-mail for the following UID: " + uid )
      logging.debug( cmd )
      
      return None
   
   if output:
      
      for item in output.split( '\n' ):
         
         if 'mail: ' in item:
            
            user_mail_adr = item[6:]
            
            if user_mail_adr:
               return user_mail_adr
   
   logging.warning( "No mail address retrieved from ldapsearch for UID: " + uid )
   
   return None


def main():

   parser = argparse.ArgumentParser( description='Checks for large files and sends e-mail notifications.' )
   parser.add_argument( '-f', '--config-file', dest='config_file', type=str, required=True, help='Path of the config file.' )
   parser.add_argument( '-D', '--enable-debug', dest='enable_debug', required=False, action='store_true', help='Enables logging of debug messages.' )
   parser.add_argument( '--create-table', dest='create_table', required=False, action='store_true', help='If set the notifiers table is created.' )
   parser.add_argument( '--no-mail', dest='no_mail', required=False, action='store_true', help='Disables mail send.' )
   
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
   
   large_file_size      = config.get( 'check', 'file_size' )
   threshold            = calc_threshold( large_file_size )
   file_system          = config.get( 'check', 'file_system' )
   check_interval_days  = int( config.get( 'check', 'check_interval_days' ) )
   
   notify_table         = config.get( 'notify', 'table' )
   notify_database      = config.get( 'notify', 'database' )
   
   rbh_database         = config.get( 'mysqld', 'database' )
   
   mail_server             = config.get( 'mail', 'server' )
   mail_sender             = config.get( 'mail', 'sender' )
   mail_subject            = config.get( 'mail', 'subject' ) + " - " + file_system
   mail_overview_recipient = config.get( 'mail', 'overview_recipient' )
   mail_user_notification  = config.get( 'mail', 'send_user_mail' )
   
   ldap_server          = config.get( 'ldap', 'server' )
   ldap_dc              = config.get( 'ldap', 'dc' )
   
   if not args.no_mail:
      smtp_conn = smtplib.SMTP( mail_server )
   
   with closing( MySQLdb.connect( host=config.get( 'mysqld', 'host' ), user=config.get( 'mysqld', 'user' ), passwd=config.get( 'mysqld', 'password' ), db=rbh_database ) )  as conn:
      with closing( conn.cursor() ) as cur:
         
         conn.autocommit( True )
         
         entries_table_handler  = EntriesTableHandler( cur, logging, rbh_database, threshold, file_system )
         notifier_table_handler = NotifierTableHandler( cur, logging, notify_table, notify_database )
            
         if args.create_table:
            notifier_table_handler.create_notifier_table()
         
         entry_info_map = entries_table_handler.get_entry_info_map()
         
         check_timestamp = datetime.datetime.fromtimestamp( time.time() ).strftime( '%Y-%m-%d %H:%M:%S' )
         
         overview_report_buf = StringIO()
         
         if entry_info_map:
            
            num_entries = 0
            
            for uid in entry_info_map.keys():
               
               user_report_buf = StringIO()
               
               new_notify_info_list    = list()
               update_notify_info_list = list()
               last_notify             = None
               
               for entry_info in entry_info_map[ uid ]:
                  
                  # TODO: Get a list for each user instead...
                  notify_item = notifier_table_handler.get_notify_item( entry_info.fid )
               
                  if notify_item:
                     
                     if notify_item.ignore_notify == 'TRUE':
                        continue
                     
                     last_notify_check = notify_item.last_notify

                     if last_notify_check == 'NULL':

                        logging.debug('Retrieved empty notify_item.last_notify!')

                        last_notify_check = datetime.datetime( 1970, 1, 1, 00, 00, 00 )
                     
                     last_notify_threshold = last_notify_check + datetime.timedelta( days = check_interval_days )
                     
                     if last_notify_threshold < datetime.datetime.fromtimestamp( time.time() ):
                        
                        notify_info = NotifyInfo( entry_info.fid, entry_info.uid, entry_info.size, entry_info.path, check_timestamp, notify_item.last_notify )
                        
                        update_notify_info_list.append( notify_info )
                        
                        user_report_buf.write( notify_info.export_compact_to_csv() )
                     
                     notifier_table_handler.update_notify_item_on_last_check( notify_item, entry_info, check_timestamp )

                  else:
                     
                     new_notify_info = NotifyInfo( entry_info.fid, entry_info.uid, entry_info.size, entry_info.path, check_timestamp )
                     
                     new_notify_info_list.append( new_notify_info )
                     
                     user_report_buf.write( new_notify_info.export_compact_to_csv() )
               
               large_file_list = user_report_buf.getvalue()
               
               if large_file_list:
                  
                  mail_receiver = uid_to_mail_ldap( ldap_server, ldap_dc, uid )
                  
                  if not args.no_mail and mail_user_notification == 'on' and mail_receiver:
                  
                     try:
                        
                        mail_body = create_user_mail_body( uid, file_system, large_file_size, large_file_list )
                        
                        smtp_conn.sendmail( mail_sender, mail_receiver, create_mail( mail_sender, mail_subject, mail_receiver, mail_body ) )
                        logging.info( "An user report has been sent to: " + mail_receiver )
                        
                        last_notify = datetime.datetime.fromtimestamp( time.time() ).strftime( '%Y-%m-%d %H:%M:%S' )
                        
                        for notify_info in new_notify_info_list:
                           notify_info.last_notify = last_notify
                           
                        for notify_info in update_notify_info_list:
                           notify_info.last_notify = last_notify
                        
                     except smtplib.SMTPException:
                        logging.error( "No user notification mail could be sent to: " + mail_receiver )
                  
                  if new_notify_info_list:
                     notifier_table_handler.insert_new_notify_info_list( new_notify_info_list )
                  
                  if update_notify_info_list and last_notify:
                     notifier_table_handler.update_last_notify( update_notify_info_list, last_notify )
               
               for notify_info in new_notify_info_list:
                  overview_report_buf.write( notify_info.export_full_to_csv() )
                  
               for notify_info in update_notify_info_list:
                  overview_report_buf.write( notify_info.export_full_to_csv() )
               
               # One additional line break after a user specific file list in the overview report.
               if new_notify_info_list or update_notify_info_list:
                  overview_report_buf.write( '\n' )
            
               user_report_buf.close()
            
            entries_table_handler.reset_fid_map()
            
            overview_report_list = overview_report_buf.getvalue()
            
            if not args.no_mail and overview_report_list:
               
               try:
                  
                  mail_body = """Dear All,\n
this is the automated report of stored large files on '""" + file_system + """' that are equal or larger than """ + large_file_size + """.\n
The following information is provided in CSV format: uid;size;path;last_notify\n\n""" + overview_report_list + """\n"""
                     
                  smtp_conn.sendmail( mail_sender, mail_overview_recipient, create_mail( mail_sender, mail_subject, mail_overview_recipient, mail_body ) )
                  logging.info( "An overview report has been sent to: " + mail_overview_recipient )
         
               except smtplib.SMTPException:
                  logging.error( "No overview report could be sent to: " + mail_overview_recipient )
         
         else:
            
            logging.info( 'No large files were found!' )
            
            if notifier_table_handler.is_table_empty():
               notifier_table_handler.truncate_table()
   
         notifier_table_handler.purge_old_table_entries( check_timestamp )
   
   logging.info( 'END' )
   
   if not args.no_mail:
      smtp_conn.quit()
   
   # TODO Error no Error occurred...
   
   return 0

if __name__ == '__main__':
   main()
