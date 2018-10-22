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
import time
import sys
import os

from contextlib import closing
from lib.retrieve_quota import retrieve_group_quota


def raise_option_not_found(section, option):
   raise Exception("Option: %s not found in section: %s" % (option, section))


def create_group_quota_history_table(cur, db, table):
   pass


def retrieve_group_names(config):

    group_names = list()

    rbh_db = config.get('robinhood', 'database')
    rbh_acct_table = config.get('robinhood', 'acct_stat_table')

    with closing(MySQLdb.connect(host=config.get('mysqld', 'host'),
                                 user=config.get('mysqld', 'user'),
                                 passwd=config.get('mysqld', 'password'),
                                 db=rbh_db)) as connection:
        
        with closing(connection.cursor()) as cursor:
            
            sql = "SELECT gid FROM %s WHERE type='file' GROUP BY 1" \
                % rbh_acct_table
            
            cursor.execute(sql)
            
            if not cursor.rowcount:
                raise RuntimeError("No rows returned from query: %s" % sql)

            for gid in cursor.fetchall():

                logging.debug("Found GID: %s" % gid[0])
                group_names.append(str(gid[0]))
    
    return group_names


def take_group_quota_snapshot(cur, 
                              rbh_db, 
                              rbh_acct_table, 
                              history_db, 
                              history_acct_table, 
                              date):
       
   pass


def main():

    # Default run-mode: collect
    run_mode = 'collect'

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('-f', '--config-file', dest='config_file', type=str,
        required=True, help='Path of the config file.')
    
    parser.add_argument('-m', '--run-mode', dest='run_mode', type=str,
        default=run_mode, required=False,
        help="Specifies the run mode: 'print' or 'collect' - Default: %s" %
            run_mode)

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

    if not (args.run_mode == 'print' or args.run_mode == 'collect'):
        raise RuntimeError("Invalid run mode: %s" % args.run_mode)
    else:
        run_mode = args.run_mode

    try:
        logging.info('START')
        
        config = ConfigParser.ConfigParser()
        config.read(args.config_file)

        fs = config.get('lustre', 'file_system')

        group_quota_map = dict()

        group_names = retrieve_group_names(config)
        
        for gid in group_names:

            try:
                quota = retrieve_group_quota(gid, fs)

                logging.debug("GID: %s - Quota: %d" % (gid, quota))

                group_quota_map[gid] = quota

            except Exception as e:

                logging.error("Skipped quota for group: %s" % gid)

                # TODO: Could be a util class for execption handling...
                exc_type, exc_obj, exc_tb = sys.exc_info()
                filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

                logging.error("Exception (type: %s): %s - %s (line: %s)" %
                    (exc_type, str(e), filename, exc_tb.tb_lineno))

        if run_mode == 'print':

            for key, value in group_quota_map.iteritems():
                print("%s:%s" % (key, value))

        if run_mode == 'collect':
            pass

        #
        # history_db = config.get('history', 'database')
        # history_acct_table = config.get('history', 'group_quota_history_table')
        #
        # with closing(MySQLdb.connect(host=config.get('mysqld', 'host'),
        #                              user=config.get('mysqld', 'user'),
        #                              passwd=config.get('mysqld', 'password'),
        #                              db=rbh_db)) as conn:
        #
        #     with closing(conn.cursor()) as cur:
        #         conn.autocommit(True)
        #
        #         if args.create_table:
        #             # create_acct_history_table(cur, history_db, history_acct_table)
        #             pass
        #
        #         date_today = time.strftime('%Y-%m-%d')
        
        logging.info('END')
        
        return 0
        
    except Exception as e:
    
        exc_type, exc_obj, exc_tb = sys.exc_info()
        filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

        logging.error("Exception (type: %s): %s - %s (line: %s)" %
            (exc_type, str(e), filename, exc_tb.tb_lineno))


if __name__ == '__main__':
    main()