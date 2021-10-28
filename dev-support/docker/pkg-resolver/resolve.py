#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Platform package dependency resolver for building Apache Hadoop.
"""

import json
import os
import sys
from check_platform import is_supported_platform


def get_packages(platform):
    """
    Resolve and get the list of packages to install for the given platform.

    :param platform: The platform for which the packages needs to be resolved.
    :return: A list of resolved packages to install.
    """
    with open('pkg-resolver/packages.json', mode='rb') as pkg_file:
        pkgs = json.loads(pkg_file.read().decode("UTF-8"))
    packages = []
    for platforms in [x for x in pkgs.values() if x.get(platform) is not None]:
        if isinstance(platforms.get(platform), list):
            packages.extend(platforms.get(platform))
        else:
            packages.append(platforms.get(platform))
    return packages


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write('ERROR: Need at least 1 argument, {} were provided{}'.format(
            len(sys.argv) - 1, os.linesep))
        sys.exit(1)

    PLATFORM_ARG = sys.argv[1]
    if not is_supported_platform(PLATFORM_ARG):
        sys.stderr.write(
            'ERROR: The given platform {} is not supported. '
            'Please refer to platforms.json for a list of supported platforms{}'.format(
                PLATFORM_ARG, os.linesep))
        sys.exit(1)

    print ' '.join(get_packages(PLATFORM_ARG))
