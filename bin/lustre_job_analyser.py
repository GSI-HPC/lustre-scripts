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
import sys
import os


def create_job_stats_file(args):

    with open(args.path_jobstats_file, 'w') as jobstats_file:

        jobstats_call = "lctl get_param *.*.job_stats | show_high_jobstats.pl -o " + str(args.min_samples)

        # TODO: Use clush API to detect errors and getting the output.
        jobstats_output = subprocess.check_output(['clush', '-l', args.clush_user, '-w', args.oss_nodes, jobstats_call],
                                                  stderr=subprocess.STDOUT)

        jobstats_output_lines = jobstats_output.splitlines()

        for jobstat_line in jobstats_output_lines:

            logging.debug("Retrieved line from show_high_jobstats: %s" % jobstat_line)

            if create_jobstat_item(jobstat_line):
                logging.debug("Writing jobstat line to file: %s" % jobstat_line)

                jobstats_file.write(jobstat_line + "\n")


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

        logging.warning("Skipping jobstat line: '%s'." % jobstat_line)

        return None

    oss_name = jobstat_item_fields[0].split(':')[0]
    job_id = int(jobstat_item_fields[2])
    sample_count = int(jobstat_item_fields[5])
    operation_type = jobstat_item_fields[6]

    return JobstatItem(oss_name, job_id, sample_count, operation_type)


class OSSStatItem:

    def __init__(self, oss_name, read_samples, write_samples):

        self.oss_name = oss_name
        self.read_samples = read_samples
        self.write_samples = write_samples

    def to_string(self):
        return ("%s:%s:%s" % (self.oss_name, self.read_samples, self.write_samples))


class JobStatInfoItem:

    def __init__(self, job_id):

        self.job_id = job_id
        self.oss_stat_item_dict = dict()


def create_job_stat_info_item_dict(path_jobstats_file):

    if os.path.isfile(path_jobstats_file):

        with open(path_jobstats_file, 'r') as jobstats_file:

            logging.debug('Processing jobstat file for initializing job_stat_info_item_dict.')

            job_stat_info_item_dict = dict()

            for jobstat_line in jobstats_file:

                logging.debug(jobstat_line)

                jobstat_item = create_jobstat_item(jobstat_line)

                read_samples, write_samples = None, None

                if jobstat_item.operation_type == 'read':
                    read_samples = jobstat_item.sample_count

                if jobstat_item.operation_type == 'write':
                    write_samples = jobstat_item.sample_count

                oss_stat_item = OSSStatItem(jobstat_item.oss_name, read_samples, write_samples)

                job_id = jobstat_item.job_id

                if job_id in job_stat_info_item_dict:

                    job_stat_info_item = job_stat_info_item_dict[job_id]

                    if oss_stat_item.oss_name in job_stat_info_item.oss_stat_item_dict:

                        oss_stat_item = job_stat_info_item.oss_stat_item_dict[oss_stat_item.oss_name]

                        if read_samples is not None:
                            oss_stat_item.read_samples = read_samples
                        if write_samples is not None:
                            oss_stat_item.write_samples = write_samples

                    else:
                        job_stat_info_item.oss_stat_item_dict[oss_stat_item.oss_name] = oss_stat_item

                else:

                    job_stat_info_item = JobStatInfoItem(job_id)
                    job_stat_info_item.oss_stat_item_dict[oss_stat_item.oss_name] = oss_stat_item

                    job_stat_info_item_dict[job_id] = job_stat_info_item

            return job_stat_info_item_dict

    else:
        raise RuntimeError('No jobstat file found for further processing!')


class SQueueInfoItem:

    def __init__(self, base_job_id, job_id, user, group, node, command):

        self.base_job_id = base_job_id
        self.job_id = job_id
        self.user = user
        self.group = group
        self.node = node
        self.command = command

    def is_job_array_job(self):

        if self.base_job_id != self.job_id:
            return True
        else:
            return False


def create_squeue_info_item(squeue_line):

    if squeue_line is None or squeue_line == '':
        raise RuntimeError('Argument for squeue_line was empty!')

    squeue_info_item_fields = squeue_line.split('|')

    if len(squeue_info_item_fields) != 6:
        raise RuntimeError("Invalid number of fields detected in squeue item: %s" % squeue_line)

    base_job_id = int(squeue_info_item_fields[0])
    job_id = int(squeue_info_item_fields[1])
    user = squeue_info_item_fields[2]
    group = squeue_info_item_fields[3]
    node = squeue_info_item_fields[4]
    command = squeue_info_item_fields[5]

    return SQueueInfoItem(base_job_id, job_id, user, group, node, command)


def create_squeue_info_list(args, job_id_list):

    # TODO: Check count of job ids and maybe split the list for multiple squeue calls to do not overloading the SLURM controller!
    len_job_id_list = len(job_id_list)

    job_id_csv = ''

    if len_job_id_list > 0:

        job_id_csv = str(job_id_list[0])

        if len_job_id_list > 1:

            for i in xrange(1, len_job_id_list):
                job_id_csv += ',' + str(job_id_list[i])

        if job_id_csv == '':
            raise RuntimeError('job_id_csv should not be empty!')

        logging.debug('Retrieving information about jobs located in the SLURM scheduling queue...')

        squeue_call = "squeue --noheader --sort 'i' --format '%F|%A|%u|%g|%N|%o' -j " + job_id_csv

        logging.debug(squeue_call)

        user_at_host = args.clush_user + '@' + args.cmp_node

        squeue_output = subprocess.check_output(['ssh', user_at_host, squeue_call], stderr=subprocess.STDOUT)

        return squeue_output.lstrip().splitlines()


class JobInfoItem:

    def __init__(self, job_id, user, group, command):

        self.job_id = job_id
        self.user = user
        self.group = group
        self.command = command
        self.oss_set = set()
        self.node_set = set()

    def to_string(self):

        output_string = str(self.job_id) + "|" + \
                        self.user + "|" + \
                        self.group + "|" + \
                        self.command + "|"

        # Since no indexed access is possible in a set, iterate over it with a counter evaluation...
        item_counter = 1

        for node in self.node_set:

            if item_counter == 1:
                output_string += node
            else:
                output_string += ";" + node

            item_counter += 1

        output_string += '|'

        item_counter = 1

        for oss in self.oss_set:

            if item_counter == 1:
                output_string += oss
            else:
                output_string += ";" + oss

            item_counter += 1

        return output_string


def init_arg_parser():

    parser = argparse.ArgumentParser(description='Lustre Job Analyser')

    parser.add_argument('-D', '--enable-debug', dest='enable_debug', required=False, action='store_true',
                        help='Enables debug log messages.')

    parser.add_argument('-u', '--clush-user', dest='clush_user', type=str, required=True,
                        help='User for executing clush commands.')

    parser.add_argument('-s', '--oss-nodes', dest='oss_nodes', type=str, required=True,
                        help='Specification of OSS nodes by using ClusterShell NodeSet syntax.')

    parser.add_argument('-n', '--cmp-node', dest='cmp_node', type=str, required=True,
                        help='Specification of Compute Node.')

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

        if args.create_jobstats_file:
            create_job_stats_file(args)

        job_stat_info_item_dict = create_job_stat_info_item_dict(args.path_jobstats_file)

        len_job_stat_info_item_dict = len(job_stat_info_item_dict)

        logging.debug("len(job_stat_info_item_dict): %s" % len_job_stat_info_item_dict)

        if len_job_stat_info_item_dict > 0:

            squeue_info_list = create_squeue_info_list(args, job_stat_info_item_dict.keys())

            job_info_item_dict = dict()

            for squeue_info_line in squeue_info_list:

                logging.debug(squeue_info_line)

                squeue_info_item = create_squeue_info_item(squeue_info_line)

                if squeue_info_item.job_id in job_stat_info_item_dict:

                    job_stat_info_item = job_stat_info_item_dict[squeue_info_item.job_id]

                    job_info_item = None

                    if squeue_info_item.base_job_id not in job_info_item_dict:

                        job_info_item = \
                            JobInfoItem(squeue_info_item.job_id,
                                        squeue_info_item.user,
                                        squeue_info_item.group,
                                        squeue_info_item.command)

                        job_info_item_dict[squeue_info_item.base_job_id] = job_info_item

                    else:
                        job_info_item = job_info_item_dict[squeue_info_item.base_job_id]

                    job_info_item.oss_set = set(job_stat_info_item.oss_stat_item_dict.keys())
                    job_info_item.node_set.add(squeue_info_item.node)

                else:
                    logging.warning("squeue_info_item.job_id not found in job_stat_info_item_dict: %s (Base Job-ID: %s)"
                                    % (squeue_info_item.job_id, squeue_info_item.base_job_id))

            for job_info_item in job_info_item_dict.values():
                print(job_info_item.to_string())

        else:
            logging.info("Jobstat item list is empty... Nothing to do!")

        logging.debug('END')

    except Exception as e:

        exc_type, exc_obj, exc_tb = sys.exc_info()
        filename = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

        logging.error("Caught exception in main function: %s - %s (line: %s)" % (str(e), filename, exc_tb.tb_lineno))

        os._exit(1)

    os._exit(0)



if __name__ == '__main__':
    main()
