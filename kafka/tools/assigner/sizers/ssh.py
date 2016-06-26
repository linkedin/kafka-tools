from kafka.tools.assigner import log
from kafka.tools.assigner.sizers import SizerModule

import paramiko
import re


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
    sizes = {}
    size_re = re.compile("^([0-9]+)\s+.*?\/([a-z0-9_-]+)-([0-9]+)\s*$", re.I)
    for broker_id, broker in self.cluster.brokers.iteritems():
      log.info("Getting partition sizes via SSH for {0}".format(broker.hostname))
      self._client.connect(broker.hostname, allow_agent=True)
      stdin, stdout, stderr = self._client.exec_command('du -sk {0}/*'.format(args.datadir))
      for ln in stdout.readlines():
        m = size_re.match(ln)
        if m:
          size = int(m.group(1))
          topic = m.group(2)
          pnum = int(m.group(3))

          if topic not in cluster.topics:
            log.warn("Unknown topic found on disk on broker {0}: {1}".format(broker, topic))
          elif pnum >= len(self.cluster.topics[topic].partitions):
            log.warn("Unknown partition found on disk on broker {0}: {1}:{2}".format(broker, topic, pnum))
          else:
            self.cluster.topics[topic].partitions[pnum].set_size(size)
    self._client.close()
