#!/usr/bin/python

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
                values.append(float(fields[6]))

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

for val in values:
    sum_values += val

avg = round(sum_values / len(values), 2)

print("avg: %s" % avg)
