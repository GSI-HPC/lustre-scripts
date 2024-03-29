#!/usr/bin/env python3
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

from datetime import datetime

from lib.clush.RangeSet import RangeSet
from lib.version.minimal_python import MinimalPython

DEFAULT_FILENAME_EXT = '.unl'
HELP_FILENAME_PATTERN = "file_class_ost{INDEX}"

REGEX_STR_HEADER = r"^\s*type,\s*size,\s*path,\s*stripe_cnt,\s*stripe_size,\s*pool,\s*stripes,\s*data_on_ost(\d+)$"
REGEX_STR_BODY   = r"^\s*file,[^,]+,\s*(.+),\s+\d+,\s+\d+,[^,]+,\s*ost.*,[^,]+$"
REGEX_STR_TAIL   = r"^Total: \d+ entries, \d+ bytes .*$"
REGEX_STR_CHUNKS = r"^(\d{1,2})\/(\d{1,2})$"
REGEX_PATTERN_HEADER   = re.compile(REGEX_STR_HEADER)
REGEX_PATTERN_BODY     = re.compile(REGEX_STR_BODY)
REGEX_PATTERN_TAIL     = re.compile(REGEX_STR_TAIL)
REGEX_PATTERN_CHUNKS   = re.compile(REGEX_STR_CHUNKS)

def init_logging(log_filename, enable_debug):

    if enable_debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    if log_filename:
        logging.basicConfig(filename=log_filename, level=log_level, format="%(asctime)s - %(levelname)s: %(message)s")
    else:
        logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s: %(message)s")

def build_chunks(chunks : str) -> tuple[int:int]:

    matched = REGEX_PATTERN_CHUNKS.match(chunks)

    if not matched:
        raise RuntimeError(f"No regex match for chunks {chunks}")

    n = int(matched.group(1))
    m = int(matched.group(2))

    if n == 0 or m == 0:
        raise ValueError('Chunks values are not allowed to be 0')

    if n >= m:
        raise ValueError(f"Invalid chunk values, n ({n}) must be less than m ({m})")

    return tuple((n, m))

def main():

    MinimalPython.check()

    parser = argparse.ArgumentParser(description='Creates mapping files between start OST index and filepath based on Robinhood unloads generated with rbh-report.')
    parser.add_argument('-e', '--filename-ext', dest='filename_ext', type=str, required=False, default=DEFAULT_FILENAME_EXT, help=f"Default: {DEFAULT_FILENAME_EXT}")
    parser.add_argument('-f', '--filename-pattern', dest='filename_pattern', type=str, required=False, help=f"For instance: {HELP_FILENAME_PATTERN}, where {{INDEX}} is a placeholder for the OST index.")
    parser.add_argument('-i', '--ost-indexes', dest='ost_indexes', type=str, required=False, help='Defines a RangeSet for the OST indexes e.g. 0-30,75,87-103')
    parser.add_argument('-c', '--chunks', dest='chunks', type=str, required=False, help='If N/M e.g. 7/10 is defined, N must be smaller than M, so N lines are transformed of M lines.')
    parser.add_argument('-w', '--work-dir', dest='work_dir', type=str, required=False, help='Specifies working directory which contains unload files')
    parser.add_argument('-x', '--exact-filename', dest='exact_filename', type=str, required=False, help='Explicit filename to process.')
    parser.add_argument('-l', '--log-file', dest='log_file', type=str, required=False, help='Specifies logging file.')
    parser.add_argument('-D', '--enable-debug', dest='enable_debug', required=False, action='store_true', help='Enables logging of debug messages.')

    args = parser.parse_args()

    init_logging(args.log_file, args.enable_debug)

    logging.info('STARTED')

    unload_files : list[str] = []
    chunk_n : int = 1
    chunk_m : int = 1

    if (args.filename_pattern and not args.ost_indexes) or (args.ost_indexes and not args.filename_pattern):
        raise RuntimeError('If any of filename-pattern or ost-indexes is set, both must be set')

    if args.exact_filename and args.work_dir:
        raise RuntimeError('Parameter exact-filename and work-dir cannot be set at the same time')

    if args.exact_filename:
        if os.path.isfile(args.exact_filename):
            unload_files.append(args.exact_filename)

    elif args.ost_indexes:

        if not "{INDEX}" in args.filename_pattern:
            raise RuntimeError("{INDEX} field must be contained in the filename-pattern argument")

        for index in list(RangeSet(args.ost_indexes).striter()):
            filename = args.filename_pattern.replace('{INDEX}', index, 1) + args.filename_ext
            unload_file = os.path.join(args.work_dir, filename)

            if os.path.isfile(unload_file):
                unload_files.append(unload_file)

    else:
        for filename in os.listdir(args.work_dir):
            if filename.endswith(args.filename_ext):
                unload_files.append(os.path.join(args.work_dir, filename))

    if args.chunks:
        chunk_n, chunk_m = build_chunks(args.chunks)

    if not unload_files:
        logging.info('No unload files have been found')

    for unload_file in unload_files:

        input_file = f"{unload_file.rsplit('.', 1)[0]}.input"
        logging.debug("Creating input file: %s", input_file)

        found_header = False
        found_tail = False
        ost_index = None
        line_number = 0
        error_counter = 0
        chunk_counter = 0

        with open(unload_file, 'rb') as reader:
            with open(input_file, 'w', encoding='utf8') as writer:

                start_time = datetime.now()

                for raw_line in reader:

                    matched = None

                    line_number += 1

                    try:
                        line = raw_line.decode(errors='strict')
                    except UnicodeDecodeError as e:
                        line = raw_line.decode(errors='replace')
                        logging.error(f"Decoding failed for line ({line_number}): {line}")
                        error_counter += 1

                    if found_header and not found_tail:

                        if not line.strip():
                            continue

                        matched = REGEX_PATTERN_BODY.match(line)

                        if matched:

                            # Default no chunks: chunk_n, chunk_m = 1
                            if chunk_n != chunk_m:
                                chunk_counter += 1

                            # Default no chunks: chunk_counter = 0
                            # With chunks chunk_counter will change
                            if chunk_counter <= chunk_n:
                                writer.write(f"{ost_index} {matched.group(1)}\n")

                            if chunk_counter == chunk_m:
                                chunk_counter = 0

                        else:

                            matched = REGEX_PATTERN_TAIL.match(line)

                            if matched:
                                found_tail = True
                            else:
                                logging.error(f"No regex match for line ({line_number}): {line}")
                                error_counter += 1
                                continue

                    elif not found_header and not found_tail:

                        matched = REGEX_PATTERN_HEADER.match(line)

                        if matched:
                            found_header = True
                            ost_index = matched.group(1)
                        else:
                            logging.debug(f"Skipping line before header: {line}")

                    elif found_tail:
                        logging.error('Inconsistent file... Tail already found.')
                        error_counter += 1
                        break
                    else:
                        raise RuntimeError('Undefined state') # For completeness.

                time_elapsed = datetime.now() - start_time
                logging.debug("Time elapsed: %s", time_elapsed)

        if found_header == False:
            logging.error(f"No header found - Failed processing unload file: {unload_file}")
            error_counter += 1
        if found_tail == False:
            logging.error(f"No tail found - Failed processing unload file: {unload_file}")
            error_counter += 1

        if error_counter > 0:
            logging.error(f"Detected {error_counter} errors for file {unload_file}")

    logging.info('FINISHED')

if __name__ == '__main__':
    main()
