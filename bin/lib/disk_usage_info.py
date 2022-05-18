#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  disk_usage_info.py
#
#  Gabriele Iannetti <g.iannetti@gsi.de>

import logging
import subprocess

from decimal import Decimal




def lustre_ost_disk_usage_info_decimal_base_2( path ):
   
   try:
      output = subprocess.check_output( ["lfs", "df", path], stderr=subprocess.STDOUT )
      
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
      
      if total_size_ost:
         return total_size_ost
      else:
         raise RuntimeError("Total OST size is 0!")
   
   except subprocess.CalledProcessError as e:

      logging.error( e.output )
      
      raise RuntimeError( e.output )
