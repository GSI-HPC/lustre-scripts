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


import re
import os
import subprocess


LFS_BIN = '/usr/bin/lfs'


def retrieve_lfs_group_quota(gid, fs):

    output = subprocess.check_output([LFS_BIN, "quota", "-g", gid, fs])
    
    return extract_soft_quota(output)


def retrieve_lfs_user_quota(uid, fs):

    output = subprocess.check_output([LFS_BIN, "quota", "-u", uid, fs])

    return extract_soft_quota(output)


def check_lfs_binary():

    if not os.path.isfile(LFS_BIN):
        raise RuntimeError("LFS binary was not found under: '%s'" % LFS_BIN)


def extract_soft_quota(output):

    lines = output.rstrip().split('\n')

    if len(lines) != 3:
        raise RuntimeError("Output has more than 3 lines: %s." % output)

    fields_line = lines[2].strip()
    
    # Trim whitespaces and return all fields.
    fields = re.sub(r'\s+', ' ', fields_line).split(' ')

    kbytes_quota = int(fields[2])
    bytes_quota = kbytes_quota * 1024
    
    return bytes_quota

