# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import division

import json
import os
from functools import wraps

from kafka.tools.exceptions import ConfigurationException
from kafka.tools.protocol.errors import error_short


def is_exec_file(fname):
    """
    Check if the given filename is a regular file and is executable.

    :param fname: the filename to check.
    :returns: True if the filename given exists and is executable, False otherwise
    """
    return os.path.isfile(fname) and os.access(fname, os.X_OK)


def find_path_containing(fname):
    """
    Search the PATH for the given executable filename

    :param fname: the filename to check
    :return: the path that contains the filename
    :raises: ConfigurationException if the filename cannot be found, or if it is not executable
    """
    if 'PATH' in os.environ:
        for path in os.environ['PATH'].split(os.pathsep):
            path = path.strip('"')
            script_file = os.path.join(path, fname)
            if is_exec_file(script_file):
                return path
    raise ConfigurationException("Cannot find the Kafka admin utilities using PATH. Try using the --tools-path option")


def get_tools_path(tools_path=None):
    """
    Find the Kafka admin utilities, either from the provided arg or the PATH.

    :param tools_path: the path to use for locating the Kafka admin utilities.
    :return: the path that contains Kafka admin utilities
    :raises: ConfigurationException if the path cannot be determined
    """
    if tools_path is not None:
        script_file = os.path.join(tools_path, 'kafka-reassign-partitions.sh')
        if not is_exec_file(script_file):
            raise ConfigurationException("--tools-path does not lead to the Kafka admin utilities ({0} is not an executable)".format(script_file))
        return tools_path

    return find_path_containing('kafka-reassign-partitions.sh')


def check_java_home():
    """
    Make sure that JAVA_HOME in the current environment is specified and is valid.

    :raises: ConfigurationException if JAVA_HOME is not set or does not contain java
    """
    if 'JAVA_HOME' in os.environ:
        java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
        if not is_exec_file(java_bin):
            raise ConfigurationException("The JAVA_HOME environment variable doesn't seem to work ({0} is not an executable)".format(java_bin))
    else:
        raise ConfigurationException("The JAVA_HOME environment variable must be set")


def json_loads(json_str):
    """
    Load the provided string as JSON data. Make sure to try the python2 way and the python3 way

    :param json_str: The JSON encoded string
    :return: The decoded JSON object
    """
    try:
        return json.loads(json_str)
    except TypeError:
        return json.loads(json_str.decode('utf-8'))


def synchronized(item):
    """
    Decorator that synchronizes access to the instance method it decorates using a preexisting lock in the _lock
    attribute of the instance
    """
    @wraps(item)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return item(self, *args, **kwargs)
    return wrapper


def raise_if_error(klass, errnum):
    if errnum != 0:
        raise klass(error_short(errnum))
