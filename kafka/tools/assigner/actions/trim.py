from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import ConfigurationException, NotEnoughReplicasException


class ActionTrim(ActionModule):
    name = "trim"
    helpstr = "Remove partitions from some brokers (reducing RF)"

    def __init__(self, args, cluster):
        super(ActionTrim, self).__init__(args, cluster)

        for b in args.brokers:
            if b not in self.cluster.brokers:
                raise ConfigurationException("Broker (ID {0}) is not in the brokers list for this cluster".format(b))

        self.brokers = args.brokers

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument('-b', '--brokers', help="List of broker IDs to remove", required=True, type=int, nargs='*')

    def process_cluster(self):
        # For each broker specified, remove it from the replica list for all its partitions
        for broker_id in self.brokers:
            broker = self.cluster.brokers[broker_id]
            for position in broker.partitions:
                partition_list = broker.partitions[position][:]
                for partition in partition_list:
                    partition.remove_replica(broker)
                    if len(partition.replicas) < 1:
                        raise NotEnoughReplicasException("Cannot trim {0}:{1} as it would result in an empty replica list".format(
                            partition.topic.name, partition.num))
