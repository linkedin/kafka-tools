from __future__ import division

import os

from kafka.tools.assigner.exceptions import ConfigurationException


# Check if the given filename is a regular file and is executable
def is_exec_file(fname):
    return os.path.isfile(fname) and os.access(fname, os.X_OK)


# Find the Kafka admin utilities, either from the provided arg or the PATH
def get_tools_path(tools_path=None):
    if tools_path is not None:
        script_file = os.path.join(tools_path, 'kafka-reassign-partitions.sh')
        if not is_exec_file(script_file):
            raise ConfigurationException("--tools-path does not lead to the Kafka admin utilities ({0} is not an executable)".format(script_file))
        return tools_path

    if 'PATH' in os.environ:
        for path in os.environ['PATH'].split(os.pathsep):
            path = path.strip('"')
            script_file = os.path.join(path, 'kafka-reassign-partitions.sh')
            if is_exec_file(script_file):
                return path

    raise ConfigurationException("Cannot find the Kafka admin utilities using PATH. Try using the --tools-path option")


# Make sure that JAVA_HOME is specified and is valid
def check_java_home():
    if 'JAVA_HOME' in os.environ:
        java_bin = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
        if not is_exec_file(java_bin):
            raise ConfigurationException("The JAVA_HOME environment variable doesn't seem to work ({0} is not an executable)".format(java_bin))
    else:
        raise ConfigurationException("The JAVA_HOME environment variable must be set")
