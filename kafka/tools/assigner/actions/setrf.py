import random
from collections import deque
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import ConfigurationException


class ActionSetRF(ActionModule):
    name = "set-replication-factor"
    helpstr = "Increase the replication factor of the specified topics"

    def __init__(self, args, cluster):
        super(ActionSetRF, self).__init__(args, cluster)
        if args.replication_factor < 1:
            raise ConfigurationException("You cannot set replication-factor below 1")
        if args.replication_factor > self.cluster.num_brokers():
            raise ConfigurationException("You cannot set replication-factor greater than the number of brokers in the cluster")
        self.target_rf = self.args.replication_factor

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument('-t', '--topics', help='List of topics to alter', required=True, nargs='*')
        parser.add_argument('-r', '--replication-factor', help='Target replication factor', required=True, type=int)

    def process_cluster(self):
        # Randomize a broker list to use for new replicas once. We'll round robin it from here
        idlist = list(self.cluster.brokers.keys())
        random.shuffle(idlist)
        brokers = deque(idlist)

        for partition in self.cluster.partitions():
            if partition.topic.name not in self.args.topics:
                continue

            while len(partition.replicas) < self.target_rf:
                broker_id = brokers.popleft()
                brokers.append(broker_id)
                broker = self.cluster.brokers[broker_id]
                if broker in partition.replicas:
                    continue
                partition.add_replica(broker)
            while len(partition.replicas) > self.target_rf:
                partition.remove_replica(partition.replicas[-1])
