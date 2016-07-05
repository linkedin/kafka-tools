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


class AssignerException(Exception):
    errstr = "Unknown Assigner Exception"

    def __init__(self, custom_errstr=None):
        super(AssignerException, self).__init__()
        if custom_errstr is not None:
            self.errstr = custom_errstr

    def __str__(self):
        return self.errstr


class ReplicaNotFoundException(AssignerException):
    errstr = "The specified replica is not present in the partition"


class NotEnoughReplicasException(AssignerException):
    errstr = "There were not enough replicas left for a partition"


class ConfigurationException(AssignerException):
    errstr = "There was an error in the configuration provided"


class ZookeeperException(AssignerException):
    errstr = "There was an error connecting to Zookeeper"


class ClusterConsistencyException(AssignerException):
    errstr = "There is a problem with the consistency of the cluster topics and/or partitions"


class ProgrammingException(AssignerException):
    errstr = "There is an error in the structure of the code"


class ReassignmentFailedException(AssignerException):
    errstr = "The partition reassignment failed"


class UnknownBrokerException(AssignerException):
    errstr = "There is an unknown broker hostname"
