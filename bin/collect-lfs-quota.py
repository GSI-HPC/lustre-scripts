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


# TODO: Extract Redundant Utility Code.

# TODO: Add mode for user/group to collect all data for that.




import ConfigParser
import logging
import argparse
import MySQLdb
import os
import time
import smtplib

from contextlib import closing
from lib.retrieve_quota import retrieve_group_quota


def raise_option_not_found(section, option):
   raise Exception("Option: %s not found in section: %s." % (option, section))


def validate_config_file(config):
   pass


def create_group_quota_history_table(cur, db, table):
   pass



def take_group_quota_snapshot(cur, 
                              rbh_db, 
                              rbh_acct_table, 
                              history_db, 
                              history_acct_table, 
                              date):
       
   pass


def main():

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-f', '--config-file', dest='config_file', type=str,
        required=True, help='Path of the config file.')

    parser.add_argument('-D', '--enable-debug', dest='enable_debug',
        required=False, action='store_true',
        help='Enables logging of debug messages.')

    parser.add_argument('--create-table', dest='create_table',
        required=False, action='store_true',
        help='Creates the group quota history table.')

    args = parser.parse_args()

    if not os.path.isfile(args.config_file):
        raise IOError("The config file does not exist or is not a file: %s" 
            % args.config_file)
    
    logging_level = logging.INFO

    if args.enable_debug:
        logging_level = logging.DEBUG

    logging.basicConfig(level=logging_level, 
        format='%(asctime)s - %(levelname)s: %(message)s')
    
    try:
         logging.info('START')
        
        config = ConfigParser.ConfigParser()
        config.read(args.config_file)
        
        validate_config_file(config)
        
        retrieve_group_quota('hpc', '/lustre/nyx')
        
        logging.info('END')
        
        return 0
        
    except Exception as e:
        
        error_msg = str(e)
        logging.error(error_msg)


if __name__ == '__main__':
    main()
