#!/usr/bin/env python
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


import argparse
import subprocess
import logging
import time
import sys
import os


class JobstatItem:

    def __init__(self, oss_name, job_id, sample_count, operation_type):

        self.oss_name = oss_name
        self.job_id = job_id
        self.sample_count = sample_count
        self.operation_type = operation_type


def create_jobstat_item(jobstat_line):

    if jobstat_line is None or jobstat_line == '':
        raise RuntimeError('Argument for jobstat_line was empty!')

    jobstat_item_fields = jobstat_line.split()

    # Skip not relevant input of a jobstat output line.
    if len(jobstat_item_fields) != 8 or \
            jobstat_item_fields[1] != 'Job' or \
            jobstat_item_fields[3] != 'has' or \
            jobstat_item_fields[4] != 'done' or \
            jobstat_item_fields[7] != 'operations.':
        return None

    oss_name = jobstat_item_fields[0].split(':')[0]
    job_id = int(jobstat_item_fields[2])
    sample_count = int(jobstat_item_fields[5])
    operation_type = jobstat_item_fields[6]

    return JobstatItem(oss_name, job_id, sample_count, operation_type)


def init_arg_parser():

    parser = argparse.ArgumentParser(description='Lustre Job Analyser')

    parser.add_argument('-D', '--enable-debug', dest='enable_debug', required=False, action='store_true',
                        help='Enables debug log messages.')

    parser.add_argument('-u', '--clush-user', dest='clush_user', type=str, required=True,
                        help='User for executing clush commands.')

    parser.add_argument('-s', '--oss-nodes', dest='oss_nodes', type=str, required=True,
                        help='Specification of OSS in ClusterShell NodeSet Syntax.')

    parser.add_argument('-m', '--min-samples', dest='min_samples', type=int, required=True,
                        help='Minimum number of samples.')

    parser.add_argument('-C', '--create-jobstat-file', dest='create_jobstats_file', required=False, action='store_true',
                        help='Specifies if a new Lustre jobstats file should be created.')

    parser.add_argument('-j', '--path-jobstat-file', dest='path_jobstats_file', type=str, required=True,
                        help='Specifies path to save Lustre jobstats file.')

    return parser.parse_args()


def init_logging(enable_debug):

    if enable_debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s: %(message)s")


def main():

    try:

        args = init_arg_parser()

        init_logging(args.enable_debug)

        logging.debug('START')

        logging.info('Checking OSS Nodes: %s' % args.oss_nodes)
        logging.info('Looking for Minimum Sample Count in Jobs: %s' % args.min_samples)
        logging.info("Creating Lustre Jobstats File: %s", args.create_jobstats_file)
        logging.info("Path for Lustre Jobstat File: %s", args.path_jobstats_file)

        jobstats_item_list = list()

        if args.create_jobstats_file:

            with open(args.path_jobstats_file, 'w') as jobstats_file:

                jobstats_call = "lctl get_param *.*.job_stats | show_high_jobstats.pl -o " + str(args.min_samples)

                # TODO: Use clush API to detect errors and getting the output.
                jobstats_output = subprocess.check_output(['clush', '-l', args.clush_user, '-w', args.oss_nodes, jobstats_call], stderr=subprocess.STDOUT)

                jobstats_output_lines = jobstats_output.splitlines()

                for jobstat_line in jobstats_output_lines:

                    logging.debug("Retrieved line from show_high_jobstats: %s" % jobstat_line)

                    if create_jobstat_item(jobstat_line):

                        logging.debug("Writing jobstat line to file: %s" % jobstat_line)

                        jobstats_file.write(jobstat_line + "\n")

        if os.path.isfile(args.path_jobstats_file):

            with open(args.path_jobstats_file, 'r') as jobstats_file:

                logging.debug('Opened jobstat file for further processing!')

                for jobstat_line in jobstats_file:
                    jobstats_item_list.append(create_jobstat_item(jobstat_line))
        else:
            raise RuntimeError('No jobstat file found for further processing!')

        len_jobstats_item_list = len(jobstats_item_list)
        job_id_csv_list = ''

        if len_jobstats_item_list == 0:
            logging.info("Jobstat item list is empty... Nothing to do!")

        elif len_jobstats_item_list == 1:
            job_id_csv_list = jobstats_item_list[0].job_id

        else:

            job_id_csv_list = str(jobstats_item_list[0].job_id)

            for i in xrange(1, len_jobstats_item_list):
                job_id_csv_list += ',' + str(jobstats_item_list[i].job_id)

        if job_id_csv_list == '':
            raise RuntimeError('job_id_csv_list should not be empty!')

        # squeue --noheader - -sort 'u' - -format '%i|%T|%u|%g|%D|%N|%o' - j 44692654

        logging.debug('END')

    except Exception as e:

        exc_type, exc_obj, exc_tb = sys.exc_info()
        filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

        logging.error("Caught exception in main function: %s - %s (line: %s)" % (str(e), filename, exc_tb.tb_lineno))

        os._exit(1)

    os._exit(0)



if __name__ == '__main__':
    main()
