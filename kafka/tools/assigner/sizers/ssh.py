import os
import re
import subprocess
from kafka.tools import log
from kafka.tools.exceptions import UnknownBrokerException
from kafka.tools.assigner.sizers import SizerModule


class SizerSSH(SizerModule):
    name = 'ssh'
    helpstr = 'Get partition sizes by connection to each broker via SSH'

    def __init__(self, args, cluster):
        super(SizerSSH, self).__init__(args, cluster)

        self.size_re = re.compile("^([0-9]+)\s+.*?\/([a-z0-9_-]+)-([0-9]+)\s*$", re.I)

        # Add a default for datadir if it doesn't exist already
        if 'datadir' not in self.properties:
            self.properties['datadir'] = "/mnt/u001/kafka/i001_caches"

    def process_df_match(self, match_obj, broker_id):
        if match_obj:
            size = int(match_obj.group(1))
            topic = match_obj.group(2)
            pnum = int(match_obj.group(3))

            if topic not in self.cluster.topics:
                log.warn("Unknown topic found on disk on broker {0}: {1}".format(broker_id, topic))
            elif pnum >= len(self.cluster.topics[topic].partitions):
                log.warn("Unknown partition found on disk on broker {0}: {1}:{2}".format(broker_id, topic, pnum))
            else:
                self.cluster.topics[topic].partitions[pnum].set_size(size)

    def get_partition_sizes(self):
        # Get broker partition sizes
        FNULL = open(os.devnull, 'w')

        for broker_id, broker in self.cluster.brokers.items():
            if broker.hostname is None:
                raise UnknownBrokerException("Cannot get sizes for broker ID {0} which has no hostname. "
                                             "Remove the broker from the cluster before balance".format(broker_id))

            log.info("Getting partition sizes via SSH for {0}".format(broker.hostname))
            proc = subprocess.Popen(['ssh', broker.hostname, 'du -sk {0}/*'.format(self.properties['datadir'])],
                                    stdout=subprocess.PIPE, stderr=FNULL)
            for line in proc.stdout:
                self.process_df_match(self.size_re.match(line.decode()), broker_id)
