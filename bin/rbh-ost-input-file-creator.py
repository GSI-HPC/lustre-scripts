#!/usr/bin/python3
#
# Copyright 2021 Gabriele Iannetti <g.iannetti@gsi.de>
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

REGEX_STR_RBH_UNL_HEADER = r"^\s*type,\s*size,\s*path,\s*stripe_cnt,\s*stripe_size,\s*pool,\s*stripes,\s*data_on_ost(\d+)$"
REGEX_STR_RBH_UNL_BODY = r"^\s*file,[^,]+,\s*(.+),\s+\d+,\s+\d+,[^,]+,\s*ost.*,[^,]+$"
REGEX_PATTERN_RBH_UNL_HEADER = re.compile(REGEX_STR_RBH_UNL_HEADER)
REGEX_PATTERN_RBH_UNL_BODY = re.compile(REGEX_STR_RBH_UNL_BODY)

def init_logging(enable_debug):

    if enable_debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s: %(message)s")

def main():

    MinimalPython.check()

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-e', '--filename-ext', dest='filename_ext', type=str, required=False, default=DEFAULT_FILENAME_EXT, help=f"Default: {DEFAULT_FILENAME_EXT}")
    parser.add_argument('-f', '--filename-pattern', dest='filename_pattern', type=str, required=False, help=f"For instance: {HELP_FILENAME_PATTERN}")
    parser.add_argument('-i', '--ost-indexes', dest='ost_indexes', type=str, required=False, help='')
    parser.add_argument('-s', '--split-index', dest='split_index', type=int, required=False, default=1, help='')
    parser.add_argument('-w', '--work-dir', dest='work_dir', default=DEFAULT_WORK_DIR, type=str, required=False, help=f"Default: '{DEFAULT_WORK_DIR}'")
    parser.add_argument('-D', '--enable-debug', dest='enable_debug', required=False, action='store_true', help='Enables logging of debug messages.')

    args = parser.parse_args()

    init_logging(args.enable_debug)

    unload_files = list()

    if (args.filename_pattern and not args.ost_indexes) or (args.ost_indexes and not args.filename_pattern):
        raise RuntimeError('If any of filename-pattern or ost-indexes is set, both must be set.')

    if args.ost_indexes:

        if not '{INDEX}' in args.filename_pattern:
            raise RuntimeError('{INDEX} field must be contained in the filename-pattern argument.')

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

    if len(unload_files):
        logging.info(len(unload_files))
    else:
        logging.info('No unload files have been found.')

    for unload_file in unload_files:

        found_header = False
        ost_index = None

        input_file = os.path.join(args.work_dir, os.path.basename(unload_file).split('.', 1)[0] + '.input')

        logging.info(f"Creating input file: {input_file}")

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
                    except UnicodeDecodeError as err:
                        line = raw_line.decode(errors='replace')
                        logging.error(f"Decoding failed for line ({line_number}): {line}")
                        continue

                    if found_header:

                        matched = REGEX_PATTERN_RBH_UNL_BODY.match(line)

                        if not matched:
                            logging.error(f"No regex match for line ({line_number}): {line}")
                            continue

                        if split_index > 1:
                            if split_counter != split_index:
                                split_counter += 1
                                continue
                            else:
                                split_counter = 1

                        filepath = matched.group(1)
                        has_spaces = filepath.find(' ')

                        if has_spaces > 0:
                            logging.debug(f"Found whitespaces in line - Skipped line: {filepath}")
                        else:
                            writer.write(ost_index + ' ' + filepath + '\n')
                    else:
                        matched = REGEX_PATTERN_RBH_UNL_HEADER.match(line)

                        if matched:
                            found_header = True
                            ost_index = matched.group(1)

        if not found_header:
            logging.error(f"No header found - Failed processing unload file: {unload_file}")

if __name__ == '__main__':
    main()