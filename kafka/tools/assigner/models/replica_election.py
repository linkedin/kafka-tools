import json
import subprocess
from tempfile import NamedTemporaryFile


class ReplicaElection:
    def __init__(self, partitions, pause_time=300):
        self.partitions = partitions
        self.pause_time = pause_time

    def __repr__(self):
        ple = {'partitions': []}
        for partition in self.partitions:
            ple['partitions'].append(partition.dict_for_replica_election())
        return json.dumps(ple)

    def execute(self, num, total, zookeeper, tools_path, plugins=[], dry_run=True):
        if not dry_run:
            with NamedTemporaryFile() as assignfile:
                assignment_json = repr(self)
                assignfile.write(bytes(assignment_json))
                assignfile.flush()
                subprocess.call(['{0}/kafka-preferred-replica-election.sh'.format(tools_path),
                                 '--zookeeper', zookeeper,
                                 '--path-to-json-file', assignfile.name])
