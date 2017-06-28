#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  entries_table_handler.py
#  
#  Gabriele Iannetti <g.iannetti@gsi.de>


class EntryInfo:
   
   def __init__( self, fid, uid, size, path ):
      
      self.fid  = fid
      self.uid  = uid
      self.size = size
      self.path = path


class EntriesTableHandler:

   def __init__( self, cur, logger, db, threshold, file_system ):
      
      self.cur         = cur
      self.logger      = logger
      self.db          = db
      self.threshold   = threshold
      self.file_system = file_system
      self.fid_map     = dict()
   
   
   def get_entry_info_map( self ):
   
      sql = "SELECT id, uid, size FROM " + self.db + "." + "ENTRIES WHERE size >= " + str( self.threshold ) + " ORDER BY uid ASC;"
      
      self.cur.execute( sql )
      self.logger.debug( sql )
      
      self.logger.info( "Found number of large files: " + str( self.cur.rowcount ) )
      
      if not self.cur.rowcount:
         return dict()
      
      file_entries_map = dict()
      
      for row in self.cur.fetchall():
         
         uid = row[ 1 ]
         
         if uid in file_entries_map:
            
            file_path = self.get_file_path( row[ 0 ] )
            
            file_entries_map[ uid ].append( EntryInfo( row[ 0 ], row[ 1 ], row[ 2 ], file_path ) )
            
            self.logger.debug( "Appended entry info item into dict for UID: %s", uid )
            self.logger.debug( "Count of items for UID: %s", len( file_entries_map[ uid ] ) )
            
         else:
            
            file_entries_list = list()
            
            file_path = self.get_file_path( row[ 0 ] )
            
            file_entries_list.append( EntryInfo( row[ 0 ], row[ 1 ], row[ 2 ], file_path ) )
            
            file_entries_map[ uid ] = file_entries_list            
            
            self.logger.debug( "Created new list in dict for UID: %s", uid )
      
      return file_entries_map


   def get_file_path( self, fid ):
      
      return self.file_system + self.get_name_item_by_parent_fid( fid )
   
   
   def get_name_item_by_parent_fid( self, fid ):
      
      pid  = None
      name = None
      
      if fid in self.fid_map:
         
         value_tuple = self.fid_map[ fid ]
         
         pid  = value_tuple[ 0 ]
         name = value_tuple[ 1 ]
      
      else:
         sql = "SELECT parent_id, name FROM " + self.db + "." + "NAMES WHERE id = '" + fid + "'"
         
         self.cur.execute( sql )
         self.logger.debug( sql )
         
         result = self.cur.fetchone()
         
         if result:
            
            pid  = result[ 0 ]
            name = result[ 1 ]
            
            self.fid_map[ fid ] = tuple( ( pid, name ) )
      
      if pid and name:      
         return self.get_name_item_by_parent_fid( pid ) + "/" + name
      
      else:
         return ''
   
   
   def reset_fid_map( self ):
      
      self.fid_map.clear()