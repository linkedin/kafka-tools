from kafka.tools.assigner import log
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import ConfigurationException


class ActionClone(ActionModule):
    name = "clone"
    helpstr = "Copy partitions from some brokers to a new broker (increasing RF)"

    def __init__(self, args, cluster):
        super(ActionClone, self).__init__(args, cluster)

        for b in args.brokers:
            if b not in self.cluster.brokers:
                raise ConfigurationException("Source broker (ID {0}) is not in the brokers list for this cluster".format(b))
        if args.to_broker not in self.cluster.brokers:
            raise ConfigurationException("Target broker (ID {0}) is not in the brokers list for this cluster".format(args.to_broker))

        self.sources = args.brokers
        self.to_broker = self.cluster.brokers[args.to_broker]

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument('-b', '--brokers', help="List of source broker IDs", required=True, type=int, nargs='*')
        parser.add_argument('-t', '--to_broker', help="Broker ID to copy partitions to", required=True, type=int)

    def process_cluster(self):
        source_set = set(self.sources)
        for partition in self.cluster.partitions():
            if len(source_set & set([replica.id for replica in partition.replicas])) > 0:
                if self.to_broker in partition.replicas:
                    log.warn("Target broker (ID {0}) is already in the replica list for {1}:{2}".format(self.to_broker.id, partition.topic.name, partition.num))

                    # If the broker is already in the replica list, it ALWAYS becomes the leader
                    if self.to_broker != partition.replicas[0]:
                        partition.swap_replica_positions(self.to_broker, partition.replicas[0])
                else:
                    # If one of the source brokers is currently the leader, the target broker is the leader. Otherwise, the target leader is in second place
                    if partition.replicas[0].id in self.sources:
                        partition.add_replica(self.to_broker, 0)
                    else:
                        partition.add_replica(self.to_broker, 1)
