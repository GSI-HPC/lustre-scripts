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

    MAJOR = 3
    MINOR = 6
    MICRO = 8

    # See: https://docs.python.org/3/c-api/apiabiversion.html#apiabiversion
    FINAL_RELEASE_LEVEL = 240 # 0xF0

    def check():

        build_hexversion = '0x'
        build_hexversion += format(MinimalPython.MAJOR, '02x') \
                          + format(MinimalPython.MINOR, '02x') \
                          + format(MinimalPython.MICRO, '02x') \
                          + format(MinimalPython.FINAL_RELEASE_LEVEL, '02x')

        hexversion = int(build_hexversion, 16)

        if sys.hexversion < hexversion:

            found_version = f"{sys.version_info.major}." \
                            f"{sys.version_info.minor}." \
                            f"{sys.version_info.micro}-" \
                            f"{sys.version_info.releaselevel}"

            error = f"Not supported Python version found: {found_version}" \
                    f" - Minimal version required: {MinimalPython.version()}"

            raise RuntimeError(error)

    def version() -> str:
        return f"{MinimalPython.MAJOR}.{MinimalPython.MINOR}.{MinimalPython.MICRO}-final"