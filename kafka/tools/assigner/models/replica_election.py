import json
import subprocess
from tempfile import NamedTemporaryFile


class ReplicaElection:
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
                subprocess.call(['{0}/kafka-preferred-replica-election.sh'.format(tools_path),
                                 '--zookeeper', zookeeper,
                                 '--path-to-json-file', assignfile.name])
