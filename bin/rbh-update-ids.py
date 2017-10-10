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


# QUICK SOLUTION TO FIX PARALLEL UPDATES ON DATABASE (NOT OPTIMIZED SOLUTION!!!)
# TODO: IMPROVE PARALLEL UPDATES WITH SHARED TASK QUEUE...


import logging
import argparse
import threading
import MySQLdb
import pwd
import grp

from contextlib import closing


HOST = 'localhost'


class WorkerThread(threading.Thread):

   def __init__(self, uid, gid, host, username, password, database):

      threading.Thread.__init__(self)

      self.uid = uid
      self.gid = gid
      self.name = str(uid) + ":" + str(gid)

      self.host = host
      self.username = username
      self.password = password
      self.database = database

   def run(self):

      logging.debug("Thread '%s' started!" % self.name)

      self.cleanup()

      logging.debug("Thread '%s' finished!" % self.name)

   def cleanup(self):

      try:

         uid = self.uid
         gid = self.gid

         user = pwd.getpwuid(int(uid)).pw_name
         group = grp.getgrgid(int(gid)).gr_name

         do_rollback = False

         with closing(MySQLdb.connect(host=self.host, user=self.username, passwd=self.password, db=self.database)) as conn:

            with closing(conn.cursor()) as cur:

               sql = "SELECT COUNT(*) FROM ACCT_STAT WHERE uid = '" + uid + "' AND gid = '" + gid + "';"
               logging.debug(sql)
               cur.execute(sql)
               pre_count_acct_stat = int(cur.fetchone()[0])
               logging.debug(str(pre_count_acct_stat))

               sql = "SELECT COUNT(*) FROM ENTRIES WHERE uid = '" + uid + "' AND gid = '" + gid + "'"
               logging.debug(sql)
               cur.execute(sql)
               pre_count_entries = int(cur.fetchone()[0])
               logging.debug(str(pre_count_entries))

               cur.execute('BEGIN')

               sql = "UPDATE ENTRIES SET uid = '" + user + "', gid = '" + group + "' WHERE uid = '" + uid + "' AND gid = '" + gid + "'"
               logging.debug(sql)
               cur.execute(sql)
               post_count_entries = cur.rowcount
               logging.debug(str(post_count_entries))

               if pre_count_entries == post_count_entries:

                  sql = "DELETE FROM ACCT_STAT WHERE uid = '" + uid + "' AND gid = '" + gid + "';"
                  logging.debug(sql)
                  cur.execute(sql)
                  post_count_acct_stat = cur.rowcount
                  logging.debug(str(post_count_acct_stat))

                  if pre_count_acct_stat == post_count_acct_stat:
                     cur.execute('COMMIT')
                     logging.debug('COMMITED')
                  else:
                     do_rollback = True

               else:
                  do_rollback = True

               if do_rollback == True:
                  cur.execute('ROLLBACK')
                  logging.debug('ROLLBACK')

      except Exception as e:
         logging.error("Cought exception during cleanup in thread ('%s') with error message:\n%s" % (self.name, str(e)))


def get_numeric_numeric_uid_gid_list(cur):

   sql = "SELECT uid, gid FROM ACCT_STAT WHERE uid REGEXP '^[0-9]+$' AND gid REGEXP '^[0-9]+$' GROUP BY 1,2"
   logging.debug(sql)
   cur.execute(sql)

   numeric_uid_gid_list = []

   for row in cur.fetchall():
      tup = (row[0], row[1])
      numeric_uid_gid_list.append(tup)

   logging.debug("Found: %d" % len(numeric_uid_gid_list))

   return numeric_uid_gid_list

def cleanup_database(numeric_uid_gid_list, parallel_updates, host, username, password, database):

   thread_counter = 0
   thread_handles = list()

   try:

      for tup in numeric_uid_gid_list:

         th_handle = WorkerThread(tup[0], tup[1], host, username, password, database)
         th_handle.start()

         thread_handles.append(th_handle)

         thread_counter += 1

         if thread_counter == parallel_updates:

            for th_handle in thread_handles:

               logging.info("Joining thread: '%s'" % th_handle.name)

               th_handle.join()

            thread_handles = list()
            thread_counter = 0

      for th_handle in thread_handles:

         logging.debug("Joining thread: '%s'" % th_handle.name)

         th_handle.join()

   except Exception as e:
      logging.error("Cought exception during cleanup in thread ('%s') with error message:\n%s" % (self.name, str(e)))

def main():

   logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s: %(message)s')

   logging.info('START')

   parser = argparse.ArgumentParser(description='Reads numerical uid\'s and gid\'s from the Robinhood database and updates its values to corresponding alphabetical values based on LDAP.')

   parser.add_argument('-u', '--username', dest='username', type=str, required=True,  help='Username for the Robinhood Database.')
   parser.add_argument('-p', '--password', dest='password', type=str, required=True,  help='Password for the Robinhood Database.')
   parser.add_argument('-H', '--host',     dest='host',     type=str, required=False, help='Database Host.', default=HOST)
   parser.add_argument('-d', '--database', dest='database', type=str, required=True,  help='Robinhood Database.')
   parser.add_argument('-w', '--parallel-updates', dest='parallel_updates', type=int, required=False, help='Specifies parallel update count.', default=10)

   args = parser.parse_args()

   with closing(MySQLdb.connect(host=args.host, user=args.username, passwd=args.password, db=args.database)) as conn:

      with closing(conn.cursor()) as cur:

         numeric_uid_gid_list = get_numeric_numeric_uid_gid_list(cur)

         cleanup_database(numeric_uid_gid_list, args.parallel_updates, args.host, args.username, args.password, args.database)
   
   logging.info('END')

   return 0


if __name__=='__main__':
   main()