#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
# Copyright 2018 Gabriele Iannetti <g.iannetti@gsi.de>
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
import os
import time
import smtplib

from contextlib import closing


def raise_option_not_found(section, option):
   raise Exception("Option: %s not found in section: %s." % (option, section))


def validate_config_file(config):
   
   if not config.has_section('mysqld'):
      raise ConfigParser.NoSectionError('Section mysqld was not found!')

   if not config.has_option('mysqld', 'host'):
      raise_option_not_found('mysqld', 'host')

   if not config.has_option('mysqld', 'user'):
      raise_option_not_found('mysqld', 'user')
   
   if not config.has_option('mysqld', 'password'):
      raise_option_not_found('mysqld', 'password')
   
   if not config.has_section('history'):
      raise ConfigParser.NoSectionError('Section history was not found!')
   
   if not config.has_option('history', 'database'):
      raise_option_not_found('history', 'database')
   
   if not config.has_option('history', 'acct_stat_history_table'):
      raise_option_not_found('history', 'acct_stat_history_table')
   
   if not config.has_section('robinhood'):
      raise ConfigParser.NoSectionError('Section robinhood was not found!')
   
   if not config.has_option('robinhood', 'database'):
      raise_option_not_found('robinhood', 'database')
   
   if not config.has_option('robinhood', 'acct_stat_table'):
      raise_option_not_found('robinhood', 'acct_stat_table')
   
   if not config.has_section( 'mail' ):
      raise ConfigParser.NoSectionError( 'Section mail was not found!' )
   
   if not config.has_option('mail', 'server'):
      raise_option_not_found('mail', 'server')
   
   if not config.has_option('mail', 'sender'):
      raise_option_not_found('mail', 'sender')
   
   if not config.has_option('mail', 'recipient'):
      raise_option_not_found('mail', 'recipient')


def create_acct_history_table( cur, db, table ):
   
   sql = "USE " + db

   logging.debug(sql)
   cur.execute(sql)
   
   sql = """
CREATE TABLE """ + table + """ (
   uid varbinary(127) NOT NULL DEFAULT 'unknown',
   gid varbinary(127) NOT NULL DEFAULT 'unknown',
   size bigint(20) unsigned DEFAULT '0',
   count bigint(20) unsigned DEFAULT '0',
   date date NOT NULL,
   PRIMARY KEY (uid,gid,date)
) ENGINE=MyISAM DEFAULT CHARSET=latin1
"""
   
   cur.execute(sql)
   logging.debug("Created table:\n%s" % sql)


def take_acct_stat_snapshot(cur, 
                            rbh_db, 
                            rbh_acct_table, 
                            history_db, 
                            history_acct_table, 
                            date):
       
   sql = "INSERT INTO %s.%s " \
         "SELECT uid, gid, SUM(size), SUM(count), '%s' " \
         "FROM %s.%s " \
         "WHERE type='file' " \
         "GROUP BY 1,2" \
      % (history_db, history_acct_table, date, rbh_db, rbh_acct_table)
   
   logging.debug(sql)
   cur.execute(sql)
   
   if not cur.rowcount:
      raise RuntimeError("Snapshot failed for date: %s." % date)
   
   logging.info("Inserted rows: %d into table: %s for date: %s" 
      % (cur.rowcount, history_acct_table, date))


def create_mail(sender, subject, receiver, text):
   
   mail = """From: <""" + sender + """> 
To: <""" + receiver + """>
Subject: """ + subject + """ 

""" + text
   
   return mail


def main():

   parser = argparse.ArgumentParser(description='Fills RBH acct history table.')

   parser.add_argument('-f', '--config-file', dest='config_file', type=str, 
      required=True, help='Path of the config file.')
   
   parser.add_argument('-D', '--enable-debug', dest='enable_debug', 
      required=False, action='store_true', 
      help='Enables logging of debug messages.')

   parser.add_argument('--create-table', dest='create_table', 
      required=False, action='store_true', 
      help='If set the accounting history table is created.')
   
   args = parser.parse_args()
   
   if not os.path.isfile(args.config_file):
      raise IOError("The config file does not exist or is not a file: %s" 
         % args.config_file)
   
   logging_level = logging.INFO
   
   if args.enable_debug:
      logging_level = logging.DEBUG

   logging.basicConfig(level=logging_level, 
      format='%(asctime)s - %(levelname)s: %(message)s')

   logging.info('START')

   config = ConfigParser.ConfigParser()
   config.read(args.config_file)
   
   validate_config_file(config)
   
   try:
      
      rbh_db = config.get('robinhood', 'database')
      rbh_acct_table = config.get('robinhood', 'acct_stat_table')
      
      history_db = config.get('history', 'database')
      history_acct_table = config.get('history', 'acct_stat_history_table')
      
      with closing(MySQLdb.connect(host=config.get('mysqld', 'host'), 
                                   user=config.get('mysqld', 'user'), 
                                   passwd=config.get('mysqld', 'password'), 
                                   db=rbh_db)) as conn:

         with closing(conn.cursor()) as cur:
               
            conn.autocommit(True)
               
            if args.create_table:
               create_acct_history_table(cur, history_db, history_acct_table)
            
            date_today = time.strftime('%Y-%m-%d')
            
            take_acct_stat_snapshot(cur, 
                                    rbh_db, 
                                    rbh_acct_table, 
                                    history_db, 
                                    history_acct_table, 
                                    str (date_today))
      
      logging.info('END')
      
      return 0
   
   except Exception as e:
      
      error_msg = str(e)
      
      logging.error(error_msg)
      
      mail_server = config.get('mail', 'server')
      mail_sender = config.get('mail', 'sender')
      mail_recipient = config.get('mail', 'recipient')
      
      mail_subject = __file__ + " - Error Occured!"
      mail_text = error_msg
      
      mail = create_mail(mail_sender, mail_subject, mail_recipient, mail_text)
      
      smtp_conn = smtplib.SMTP(mail_server)
      smtp_conn.sendmail(mail_sender, mail_recipient, mail)
      
      logging.info("Error notification mail has been sent to: %s" % 
         mail_recipient)
      
      smtp_conn.quit()


if __name__ == '__main__':
   main()
