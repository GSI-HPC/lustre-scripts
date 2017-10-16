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

# Example Script Call: 
#
# ./rbh-set-fileclass-for-files.py -u user -p password -d database -l "{++empty_file++:size = 0},{++small_file++:size > 0 and size <= 32 KB},{++std_file++:size > 32 KB and size <= 1 GB},{++large_file++:size > 1 GB}"


import logging
import argparse
import MySQLdb
import sys
import re

from contextlib import closing


HOST = 'localhost'

FILECLASS_REG_EXP=r'^{(\+[a-zA-Z_]{1,1000}\+):((size (=|<=|>=|<|>) \d{1,2}( (B|KB|MB|GB|TB))?( and )*)+(?!size))}$'

KB_MULTIPLIER=1024
MB_MULTIPLIER=1048576
GB_MULTIPLIER=1073741824
TB_MULTIPLIER=1099511627776


def get_fileclass_definitions( fileclass_list ):
   
   fileclass_def_dict = dict()
   
   fileclass_input_list = fileclass_list.split( ',' )
   
   for fileclass_input in fileclass_input_list:

      reg_exp_match = re.match( FILECLASS_REG_EXP, fileclass_input )

      if reg_exp_match == None:
         raise RuntimeError( "The following input file class definition failed the validation: " + fileclass_input + "\n" + "Used regular expression for validation is: " + str( FILECLASS_REG_EXP ) )
      
      logging.debug( "File class input string: " + fileclass_input )
      
      fileclass_name  = reg_exp_match.group(1)
      size_definition = reg_exp_match.group(2)
      
      list_size_definition = size_definition.split(' ')
      list_size_def_count  = len( list_size_definition )
      
      sql_size_definition = str()
      
      for i in range( 0, list_size_def_count ):
         
         item = list_size_definition[ i ]
         
         if ( item == 'size' ) or ( '=' in item ) or ( '<' in item ) or ( '>' in item ):
            sql_size_definition += item
         
         elif item == 'and':
            sql_size_definition += ' AND '
         
         elif item.isdigit():
            
            if i < ( list_size_def_count - 1 ):
               
               next_item = list_size_definition[ i + 1 ]
               
               if len( next_item ) == 2 and next_item[1] == 'B':
                  
                  size_number_prepart = int( item )
                  
                  if 'KB' == next_item:
                     sql_size_definition += str( size_number_prepart * KB_MULTIPLIER )
                  elif 'MB' == next_item:
                     sql_size_definition += str( size_number_prepart * MB_MULTIPLIER )
                  elif 'GB' == next_item:
                     sql_size_definition += str( size_number_prepart * GB_MULTIPLIER )
                  elif 'TB' == next_item:
                     sql_size_definition += str( size_number_prepart * TB_MULTIPLIER )
                  else:
                     raise RuntimeError( "Found unknown unit type: " + next_item )
                  
                  next_item = None

               elif len( next_item ) == 1 and next_item[0] == 'B':
                  sql_size_definition += item
               
               else:
                  sql_size_definition += item
            
            # Reached end.
            else:
               sql_size_definition += item
         
         elif len( item ) == 2 and item[1] == 'B':
            continue

         elif len( item ) == 1 and item[0] == 'B':
            continue
         
         else:
            raise RuntimeError( "Not supported string found: " + item )
      
      logging.debug( "Created file class definition - name: '" + fileclass_name + "' and size: '" + sql_size_definition + "'" )
      
      fileclass_def_dict[ fileclass_name ] = sql_size_definition
   
   return fileclass_def_dict


def update_database( cur, fileclass_def_dict ):
   
   if len( fileclass_def_dict ) == 0:
      raise RuntimeError( 'Empty file class definition dictionary!' )
   
   names = fileclass_def_dict.keys()
   sizes = fileclass_def_dict.values()
   
   sql_update = 'UPDATE ENTRIES SET fileclass ='
   
# [START] - SQL CASE STATEMENT
   sql_update += '\nCASE\n'
   
   for name in names:
      sql_update += 'WHEN ' + fileclass_def_dict[name] + " THEN '" + name + "'\n"
   
   sql_update += 'ELSE fileclass = \'+undefined+\'\nEND\n'
# [END] - SQL CASE STATEMENT
   
# [START] - SQL WHERE CLAUSE
   sql_update += 'WHERE\n'
   
   sql_update += "( " + sizes[ 0 ] + " )"
   
   if len( sizes ) > 1:
      
      for i in range( 1, len( sizes ) ):
         sql_update += " OR\n( " + sizes[ i ] + " )"
# [END] - SQL WHERE CLAUSE
   
   logging.debug( "Executing SQL update statement:\n" + sql_update )
   
   cur.execute( sql_update )
   
   logging.info( "Updated rows: " + str ( cur.rowcount ) )


def main():

   logging.basicConfig( level=logging.DEBUG, format='%(asctime)s - %(levelname)s: %(message)s' )

   logging.info( 'START' )

   parser = argparse.ArgumentParser( description='Sets file classes for files according to a specified size. A file size is supported from bytes up to TB specification.' )

   parser.add_argument( '-u', '--username',       dest='username',       type=str, required=True,  help='Username for the Robinhood Database.' )
   parser.add_argument( '-p', '--password',       dest='password',       type=str, required=True,  help='Password for the Robinhood Database.' )
   parser.add_argument( '-H', '--host',           dest='host',           type=str, required=False, help='Database Host.', default=HOST )
   parser.add_argument( '-d', '--database',       dest='database',       type=str, required=True,  help='Robinhood Database.' )
   parser.add_argument( '-l', '--fileclass-list', dest='fileclass_list', type=str, required=True,  help='Definition of file classes in the format: {fileclass:size definition},{...} e.g. {++empty_file++:size = 0},{++small_file++:size > 0 and size <= 32 KB},{++std_file++:size > 32 KB and size <= 1 GB},{++large_file++:size > 1 GB}' )
   args = parser.parse_args()

   fileclass_def_dict = get_fileclass_definitions( args.fileclass_list )
   
   with closing( MySQLdb.connect( host=args.host, user=args.username, passwd=args.password, db=args.database ) ) as conn:

      with closing( conn.cursor() ) as cur:

         conn.autocommit( True )

         update_database( cur, fileclass_def_dict )

   update_database( None, fileclass_def_dict )
   
   logging.info( 'END' )
   
   return 0

if __name__ == '__main__':
   main()