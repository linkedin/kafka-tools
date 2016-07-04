import json
import re
import subprocess
import time
from tempfile import NamedTemporaryFile

from kafka.tools.assigner import log
from kafka.tools.assigner.exceptions import ReassignmentFailedException


class Reassignment:
    def __init__(self, partitions, pause_time=10):
        self.partitions = partitions
        self.pause_time = pause_time

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
            with NamedTemporaryFile(mode='w') as assignfile:
                json.dump(self.dict_for_reassignment(), assignfile)
                assignfile.flush()
                proc = subprocess.Popen(['{0}/kafka-reassign-partitions.sh'.format(tools_path), '--execute',
                                         '--zookeeper', zookeeper,
                                         '--reassignment-json-file', assignfile.name])
                proc.wait()

                # Wait until finished
                while True:
                    remaining_partitions = check_reassignment_completion(zookeeper, tools_path, assignfile.name)
                    if remaining_partitions == 0:
                        break

                    log.info('Partition reassignment {0}/{1} in progress [ {2}/{3} partitions remain ]. Sleeping {4} seconds'.format(num,
                                                                                                                                     total,
                                                                                                                                     remaining_partitions,
                                                                                                                                     len(self.partitions),
                                                                                                                                     self.pause_time))
                    time.sleep(self.pause_time)

        for plugin in plugins:
            plugin.after_execute_batch(num)


def check_reassignment_completion(zookeeper, tools_path, assign_filename):
    status_re = re.compile('.*Reassignment of partition.*?\s+(failed|still in progress|completed successfully)')

    proc = subprocess.Popen(['{0}/kafka-reassign-partitions.sh'.format(tools_path), '--verify',
                             '--zookeeper', zookeeper,
                             '--reassignment-json-file', assign_filename], stdout=subprocess.PIPE)
    lines = proc.stdout.readlines()

    remaining_count = 0
    for line in lines:
        m = status_re.match(line.decode())
        if m and m.group(1) == 'failed':
            raise ReassignmentFailedException("The reassignment in progress failed with the following verification output:\n{0}".format(lines))
        elif m and m.group(1) == 'still in progress':
            remaining_count += 1

    return remaining_count
