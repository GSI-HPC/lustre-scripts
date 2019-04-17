#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Copyright 2019 Gabriele Iannetti <g.iannetti@gsi.de>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#


import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input-file", dest="input_file", type=str, 
    help="Robinhood log file.")
parser.add_argument("-m", "--metric", dest="metric", type=str, 
    help="Metric to process e.g. a stage name or 'avg. speed'.")

args = parser.parse_args()

print("--- Arguments ---\nLog file: %s\nMetric: %s\n" % 
    (args.input_file, args.metric))

def get_stage_values(metric, f):

    values = list()

    for line in f:

        if metric in line:

            fields = line.split('|')

            if(len(fields)) == 8:

                value = float(fields[6])

                if value:
                    values.append(value)

    return values


sum_values = 0
values = list()

SUPPORTED_STAGES = ['GET_FID', 'GET_INFO_DB', 'GET_INFO_FS', 'PRE_APPLY',\
        'DB_APPLY', 'CHGLOG_CLR', 'RM_OLD_ENTRIES']

with open(args.input_file) as f:

    if args.metric == 'avg. speed':

        for line in f:
            
            if args.metric in line:

                result = line.split(':', 3)[3]
                scan_speed = result.split('entries')[0]
                values.append(float(scan_speed))

    elif args.metric in SUPPORTED_STAGES:
        values.extend(get_stage_values(args.metric, f))
    else:
        raise RuntimeError("Unknown or not supported metric: %s" % args.metric)

if values:

    for val in values:
        sum_values += val

    avg = round(sum_values / len(values), 2)

    print("avg: %s" % avg)

else:
    print("No values retrieved!")
