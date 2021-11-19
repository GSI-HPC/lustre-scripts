#!/usr/bin/env python3
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

from enum import Enum

import sys

class MinimalPython(int, Enum):

    # See: https://docs.python.org/3/c-api/apiabiversion.html#apiabiversion
    MAJOR = 3
    MINOR = 6
    MICRO = 8
    FINAL_RELEASE_LEVEL = 240 # 0xF0

    def check(major=MAJOR, minor=MINOR, micro=MICRO, final=FINAL_RELEASE_LEVEL):

        build_hexversion = '0x' \
                            + format(major, '02x') \
                            + format(minor, '02x') \
                            + format(micro, '02x') \
                            + format(final, '02x')

        hexversion = int(build_hexversion, 16)

        if sys.hexversion < hexversion:

            found_version = f"{sys.version_info.major}." \
                            f"{sys.version_info.minor}." \
                            f"{sys.version_info.micro}-" \
                            f"{sys.version_info.releaselevel}"

            error = f"Not supported Python version found: {found_version}" \
                    f" - Minimal version required: {MinimalPython._version(major, minor, micro)}"

            raise RuntimeError(error)

    def _version(major, minor, micro) -> str:
        return f"{major}.{minor}.{micro}-final"