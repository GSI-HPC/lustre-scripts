#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  notifier_table_handler.py
#  
#  Gabriele Iannetti <g.iannetti@gsi.de>


GB_DIV_DB=1000000000
TB_DIV_DB=1000000000000


def convert_number_human_readable( number ):
   
   number_human_readable = None
      
   if number >= GB_DIV_DB and number < TB_DIV_DB:
      number_human_readable = str ( number / GB_DIV_DB ) + "GB"
   
   elif number > TB_DIV_DB:
      number_human_readable = str( number / TB_DIV_DB ) + "TB"
   
   else:
      raise RuntimeError( 'Number convertion not supported!' )
   
   return number_human_readable


class NotifyInfo:
   
   def __init__( self, fid, uid, size, path, last_check, last_notify ):
         
         self.fid           = fid
         self.uid           = uid
         self.size          = size
         self.path          = path
         self.last_check    = last_check
         self.last_notify   = last_notify
         self.ignore_notify = 'FALSE'

   def to_sql_values(self):

      sql_values = "'" + self.fid + "', '" + self.uid + "', " + str( self.size ) + ", '" + self.path + "', '" + self.last_check + "', "

      if self.last_notify is None:
         sql_values += 'NULL'
      else:
         sql_values += "'" + self.last_notify + "'"

      sql_values += ", '" + self.ignore_notify + "'"

      return sql_values

   def export_compact_to_csv( self ):
      return convert_number_human_readable( self.size ) + ";" + self.path + "\n"

   def export_full_to_csv( self ):      
      return self.uid + ";" + convert_number_human_readable( self.size ) + ";" + self.path + ";" + self.last_notify + "\n"


class NotifierTableHandler:

   def __init__( self, cur, logger, table, db ):
      
      self.cur    = cur
      self.logger = logger
      self.table  = table
      self.db     = db
      
      self.new_notify_queue    = list()
      self.update_notify_queue = list()
   
   def create_notifier_table( self ):
      
      sql = "USE " + self.db
      
      self.cur.execute( sql )
      self.logger.debug( sql )
      
      sql = """
CREATE TABLE """ + self.table + """ (
fid varbinary(64)                   NOT NULL,
uid varbinary(127)                  NOT NULL,
size bigint(20)                     NOT NULL,
path varchar(1000)                  NOT NULL,
last_check DATETIME                 NOT NULL,
last_notify DATETIME                DEFAULT NULL,
ignore_notify ENUM('FALSE', 'TRUE') DEFAULT 'FALSE',
PRIMARY KEY (fid)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
"""
      
      self.cur.execute( sql )
      self.logger.debug( "Created table:\n" + sql )

   def is_table_empty( self ):
      
      sql = "SELECT 1 FROM " + self.db + "." + self.table + " LIMIT 1;"
   
      self.cur.execute( sql )
      self.logger.debug( sql )
      
      if self.cur.fetchone():
         return False
      else:
         return True

   def truncate_table( self ):
   
      sql = "TRUNCATE " + self.db + "." + self.table
      
      self.cur.execute( sql )
      self.logger.debug( sql )

   def get_notify_item( self, fid ):
      
      sql = "SELECT fid, uid, size, path, last_check, last_notify, ignore_notify FROM " + self.db + "." + self.table + " WHERE fid = '" + fid + "'"
      
      self.cur.execute( sql )
      self.logger.debug( sql )
      
      result = self.cur.fetchone()
      
      if result:
         return NotifyInfo( result[ 0 ], result[ 1 ], result[ 2 ], result[ 3 ], result[ 4 ], result[ 5 ], result[ 6 ] )
      
      return None

   def insert_new_notify_info_list( self, new_notify_info_list ):
      
      sql = "INSERT INTO " + self.db + "." + self.table + " VALUES "
      
      sql += "(" + new_notify_info_list[ 0 ].to_sql_values() + ")"
      
      if len( new_notify_info_list ) > 1:
         
         for notify_info in new_notify_info_list[ 1: ]:
            sql += ", (" + notify_info.to_sql_values() + ")"
      
      self.logger.debug( sql )
      self.cur.execute( sql )

   def update_last_notify( self, update_notify_info_list, last_notify ):
      
      sql = "UPDATE " + self.db + "." + self.table + " SET last_notify = '" + last_notify + "' WHERE fid IN ('" + update_notify_info_list[ 0 ].fid + "'"
      
      if len( update_notify_info_list ) > 1:
         
         for notify_info in update_notify_info_list[ 1: ]:
            sql += ", '" + notify_info.fid + "'"
      
      sql += ")"
      
      self.logger.debug( sql )
      self.cur.execute( sql )

   def update_notify_item_on_last_check( self, notify_item, entry_info, check_timestamp ):
   
      sql_update_where_list = list()
      
      if notify_item.uid != entry_info.uid:
         sql_update_where_list.append( str ( "uid = '" + entry_info.uid + "'" ) )
      
      if notify_item.path != entry_info.path:
         sql_update_where_list.append( str ( "path = '" + entry_info.path + "'" ) )
      
      if notify_item.size != entry_info.size:
         sql_update_where_list.append( str ( "size = '" + str( entry_info.size ) + "'" ) )
      
      sql = "UPDATE " + self.db + "." + self.table + " SET last_check = '" + check_timestamp + "'"
      
      if len( sql_update_where_list ) > 1:
         
         for item in sql_update_where_list:
            sql += ", " + item
      
      sql += " WHERE fid = '" + entry_info.fid + "'"
      
      self.logger.debug( sql )
      self.cur.execute( sql )

   def purge_old_table_entries( self, check_timestamp ):
      
      sql = "DELETE FROM " + self.db + "." + self.table + " WHERE last_check < '" + check_timestamp + "'"
      
      self.logger.debug( sql )
      self.cur.execute( sql )
      
      if self.cur.rowcount > 0:
         self.logger.info( "Purged old notification table entries: " + str( self.cur.rowcount ) )