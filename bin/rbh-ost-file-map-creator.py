#!/usr/bin/python3
#
# Copyright 2022 Gabriele Iannetti <g.iannetti@gsi.de>
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

import re
import argparse
import logging
import os.path

from lib.clush.RangeSet import RangeSet
from lib.version.minimal_python import MinimalPython

DEFAULT_FILENAME_EXT = '.unl'
DEFAULT_WORK_DIR = '.'
HELP_FILENAME_PATTERN = "file_class_ost{INDEX}"

REGEX_STR_HEADER = r"^\s*type,\s*size,\s*path,\s*stripe_cnt,\s*stripe_size,\s*pool,\s*stripes,\s*data_on_ost(\d+)$"
REGEX_STR_BODY = r"^\s*file,[^,]+,\s*(.+),\s+\d+,\s+\d+,[^,]+,\s*ost.*,[^,]+$"
REGEX_PATTERN_HEADER = re.compile(REGEX_STR_HEADER)
REGEX_PATTERN_BODY = re.compile(REGEX_STR_BODY)

def init_logging(log_filename, enable_debug):

    if enable_debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    if log_filename:
        logging.basicConfig(filename=log_filename, level=log_level, format="%(asctime)s - %(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s: %(message)s")

def main():

    MinimalPython.check()

    parser = argparse.ArgumentParser(description='Creates mapping files between start OST index and filepath based on Robinhood unloads generated with rbh-report.')
    parser.add_argument('-e', '--filename-ext', dest='filename_ext', type=str, required=False, default=DEFAULT_FILENAME_EXT, help=f"Default: {DEFAULT_FILENAME_EXT}")
    parser.add_argument('-f', '--filename-pattern', dest='filename_pattern', type=str, required=False, help=f"For instance: {HELP_FILENAME_PATTERN}, where {{INDEX}} is a placeholder for the OST index.")
    parser.add_argument('-i', '--ost-indexes', dest='ost_indexes', type=str, required=False, help='Defines a RangeSet for the OST indexes e.g. 0-30,75,87-103')
    parser.add_argument('-s', '--split-index', dest='split_index', type=int, required=False, default=1, help='Default: 1')
    parser.add_argument('-w', '--work-dir', dest='work_dir', default=DEFAULT_WORK_DIR, type=str, required=False, help=f"Default: '{DEFAULT_WORK_DIR}'")
    parser.add_argument('-x', '--exact-filename', dest='exact_filename', type=str, required=False, help='Explicit filename to process.')
    parser.add_argument('-l', '--log-file', dest='log_file', type=str, required=False, help='Specifies logging file.')
    parser.add_argument('-D', '--enable-debug', dest='enable_debug', required=False, action='store_true', help='Enables logging of debug messages.')

    args = parser.parse_args()

    init_logging(args.log_file, args.enable_debug)

    unload_files = list()

    if (args.filename_pattern and not args.ost_indexes) or (args.ost_indexes and not args.filename_pattern):
        raise RuntimeError('If any of filename-pattern or ost-indexes is set, both must be set.')

    if args.exact_filename:

        unload_file = os.path.join(args.work_dir, args.exact_filename)

        if os.path.isfile(unload_file):
            unload_files.append(unload_file)

    elif args.ost_indexes:

        if not '{INDEX}' in args.filename_pattern:
            raise RuntimeError("{INDEX} field must be contained in the filename-pattern argument.")

        for index in list(RangeSet(args.ost_indexes).striter()):
            filename = args.filename_pattern.replace('{INDEX}', index, 1) + args.filename_ext
            unload_file = os.path.join(args.work_dir, filename)

            if os.path.isfile(unload_file):
                unload_files.append(unload_file)

    else:
        for filename in os.listdir(args.work_dir):
            if filename.endswith(args.filename_ext):
                unload_files.append(os.path.join(args.work_dir, filename))

    if args.split_index < 1 or args.split_index > 10:
        raise RuntimeError(f"Not supported split index: {args.split_index} - Must be between 1 and 10.")

    if not unload_files:
        logging.info('No unload files have been found.')

    for unload_file in unload_files:

        input_file = os.path.join(args.work_dir, os.path.basename(unload_file).split('.', 1)[0] + '.input')
        logging.info("Creating input file: %s", input_file)

        found_header = False
        ost_index = None
        line_number = 0
        split_counter = 1
        split_index = args.split_index

        with open(unload_file, 'rb') as reader:
            with open(input_file, 'w') as writer:
                for raw_line in reader:

                    matched = None
                    line = None
                    line_number += 1

                    try:
                        line = raw_line.decode(errors='strict')
                    except UnicodeDecodeError:
                        line = raw_line.decode(errors='replace')
                        logging.error("Decoding failed for line (%i): %s", line_number, line)
                        continue

                    if found_header:

                        if not line.strip():
                            continue

                        matched = REGEX_PATTERN_BODY.match(line)

                        if not matched:
                            logging.error("No regex match for line (%i): %s", line_number, line)
                            continue

                        if split_index > 1:
                            if split_counter != split_index:
                                split_counter += 1
                                continue
                            split_counter = 1

                        writer.write(ost_index + ' ' + matched.group(1) + '\n')
                    else:
                        matched = REGEX_PATTERN_HEADER.match(line)

                        if matched:
                            found_header = True
                            ost_index = matched.group(1)

        if not found_header:
            logging.error("No header found - Failed processing unload file: %s", unload_file)

if __name__ == '__main__':
    main()
