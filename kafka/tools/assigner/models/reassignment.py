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
import re
import subprocess
import time
from tempfile import NamedTemporaryFile

from kafka.tools import log
from kafka.tools.exceptions import ReassignmentFailedException
from kafka.tools.models import BaseModel


class Reassignment(BaseModel):
    equality_attrs = ['partitions']

    def __init__(self, partitions, pause_time=10):
        self.partitions = partitions
        self.pause_time = pause_time
        self.status_re = re.compile('.*Reassignment of partition.*?\s+(failed|still in progress|completed successfully)')

    def __repr__(self):
        return json.dumps(self.dict_for_reassignment())

    def dict_for_reassignment(self):
        reassignment = {'partitions': [], 'version': 1}
        for partition in self.partitions:
            reassignment['partitions'].append(partition.dict_for_reassignment())
        return reassignment

    def execute(self, num, total, zookeeper, tools_path, plugins=[], dry_run=True):
        for plugin in plugins:
            plugin.before_execute_batch(num)
        if not dry_run:
            self._execute(num, total, zookeeper, tools_path)
        for plugin in plugins:
            plugin.after_execute_batch(num)

    def _execute(self, num, total, zookeeper, tools_path):
        with NamedTemporaryFile(mode='w') as assignfile:
            json.dump(self.dict_for_reassignment(), assignfile)
            assignfile.flush()
            FNULL = open(os.devnull, 'w')
            proc = subprocess.Popen(['{0}/kafka-reassign-partitions.sh'.format(tools_path), '--execute',
                                     '--zookeeper', zookeeper,
                                     '--reassignment-json-file', assignfile.name],
                                    stdout=FNULL, stderr=FNULL)
            proc.wait()

            # Wait until finished
            while True:
                remaining_partitions = self.check_completion(zookeeper, tools_path, assignfile.name)
                if remaining_partitions == 0:
                    break

                log.info('Partition reassignment {0}/{1} in progress [ {2}/{3} partitions remain ]. Sleeping {4} seconds'.format(num,
                                                                                                                                 total,
                                                                                                                                 remaining_partitions,
                                                                                                                                 len(self.partitions),
                                                                                                                                 self.pause_time))
                time.sleep(self.pause_time)

    def process_verify_match(self, line):
        match_obj = self.status_re.match(line)
        if match_obj:
            if match_obj.group(1) == 'failed':
                return -1
            elif match_obj.group(1) == 'still in progress':
                return 1
        return 0

    def check_completion(self, zookeeper, tools_path, assign_filename):
        FNULL = open(os.devnull, 'w')
        proc = subprocess.Popen(['{0}/kafka-reassign-partitions.sh'.format(tools_path), '--verify',
                                 '--zookeeper', zookeeper,
                                 '--reassignment-json-file', assign_filename],
                                stdout=subprocess.PIPE, stderr=FNULL)
        lines = proc.stdout.readlines()

        remaining_count = 0
        for line in lines:
            count = self.process_verify_match(line.decode())
            if count < 0:
                raise ReassignmentFailedException("The reassignment in progress failed with the following verification output:\n{0}".format(lines))
            remaining_count += count

        return remaining_count
