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

import logging
import paramiko
import re
from kafka.tools.assigner import log
from kafka.tools.assigner.exceptions import UnknownBrokerException
from kafka.tools.assigner.sizers import SizerModule


class SizerSSH(SizerModule):
    name = 'ssh'
    helpstr = 'Get partition sizes by connection to each broker via SSH'

    def __init__(self, args, cluster):
        super(SizerSSH, self).__init__(args, cluster)

        # Set up an SSH client for connecting to the brokers, and silence the logs
        self._client = paramiko.SSHClient()
        plogger = paramiko.util.logging.getLogger()
        plogger.setLevel(logging.WARNING)
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.load_system_host_keys()

    def get_partition_sizes(self):
        # Get broker partition sizes
        size_re = re.compile("^([0-9]+)\s+.*?\/([a-z0-9_-]+)-([0-9]+)\s*$", re.I)
        for broker_id, broker in self.cluster.brokers.items():
            if broker.hostname is None:
                raise UnknownBrokerException("Cannot get sizes for broker ID {0} which has no hostname. "
                                             "Remove the broker from the cluster before balance".format(broker_id))

            log.info("Getting partition sizes via SSH for {0}".format(broker.hostname))
            self._client.connect(broker.hostname, allow_agent=True)
            stdin, stdout, stderr = self._client.exec_command('du -sk {0}/*'.format(self.args.datadir))
            for ln in stdout.readlines():
                m = size_re.match(ln)
                if m:
                    size = int(m.group(1))
                    topic = m.group(2)
                    pnum = int(m.group(3))

                    if topic not in self.cluster.topics:
                        log.warn("Unknown topic found on disk on broker {0}: {1}".format(broker, topic))
                    elif pnum >= len(self.cluster.topics[topic].partitions):
                        log.warn("Unknown partition found on disk on broker {0}: {1}:{2}".format(broker, topic, pnum))
                    else:
                        self.cluster.topics[topic].partitions[pnum].set_size(size)
        self._client.close()
