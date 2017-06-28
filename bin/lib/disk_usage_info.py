#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  disk_usage_info.py
#  
#  Gabriele Iannetti <g.iannetti@gsi.de>

import commands
import logging

from decimal import Decimal




def lustre_ost_disk_usage_info_decimal_base( path ):
   
   cmd = "lfs df " + path
   
   ## TODO Deprecated!
   ( status, output ) = commands.getstatusoutput( cmd )
   
   ## Python 3.1
   ###subprocess.getstatusoutput( cmd )
   
   if status > 0:
      raise RuntimeError( "Failed to retrieve disk usage info for path: %s" % path )
   
   total_size_ost = Decimal( 0 )
   
   if output:
      
      lines = output.splitlines()
      
      # TODO Check first line: UUID                   1K-blocks        Used   Available Use% Mounted on
      
      for line in lines:
         
         if 'OST' in line:
            
            fields = line.split()
            
            ost_size = Decimal( fields[1] ) * Decimal ( 1024.0 )
            
            total_size_ost += ost_size
            
         else:
            logging.debug( "Not processed line from Lustre disk usage call: %s" % line )
   
   return total_size_ost