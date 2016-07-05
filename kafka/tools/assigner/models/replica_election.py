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

import json
import os
import subprocess
from tempfile import NamedTemporaryFile

from kafka.tools.assigner.models import BaseModel


class ReplicaElection(BaseModel):
    equality_attrs = ['partitions']

    def __init__(self, partitions, pause_time=300):
        self.partitions = partitions
        self.pause_time = pause_time

    def __repr__(self):
        return json.dumps(self.dict_for_replica_election())

    def dict_for_replica_election(self):
        ple = {'partitions': []}
        for partition in self.partitions:
            ple['partitions'].append(partition.dict_for_replica_election())
        return ple

    def execute(self, num, total, zookeeper, tools_path, plugins=[], dry_run=True):
        if not dry_run:
            with NamedTemporaryFile(mode='w') as assignfile:
                json.dump(self.dict_for_replica_election(), assignfile)
                assignfile.flush()
                FNULL = open(os.devnull, 'w')
                subprocess.call(['{0}/kafka-preferred-replica-election.sh'.format(tools_path),
                                 '--zookeeper', zookeeper,
                                 '--path-to-json-file', assignfile.name],
                                stdout=FNULL, stderr=FNULL)
