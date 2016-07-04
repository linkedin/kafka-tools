from collections import deque
from kafka.tools.assigner.actions import ActionModule
from kafka.tools.assigner.exceptions import ConfigurationException, NotEnoughReplicasException


class ActionRemove(ActionModule):
    name = "remove"
    helpstr = "Move partitions from one broker to one or more other brokers (maintaining RF)"

    def __init__(self, args, cluster):
        super(ActionRemove, self).__init__(args, cluster)

        for broker in args.brokers:
            if broker not in self.cluster.brokers:
                raise ConfigurationException("Broker to remove (ID {0}) is not in the brokers list for this cluster".format(broker))
            if (args.to_brokers is not None) and (broker in args.to_brokers):
                raise ConfigurationException("Broker to remove (ID {0}) was specified in the target broker list as well".format(broker))

        self.brokers = args.brokers
        self.to_brokers = args.to_brokers
        if (self.to_brokers is None) or (len(self.to_brokers) == 0):
            self.to_brokers = list(set(self.cluster.brokers.keys()) - set(self.brokers))
        else:
            for broker in self.to_brokers:
                if broker not in self.cluster.brokers:
                    raise ConfigurationException("Target broker (ID {0}) is not in the brokers list for this cluster".format(broker))

    @classmethod
    def _add_args(cls, parser):
        parser.add_argument('-b', '--brokers', help="List of Broker IDs to remove", required=True, type=int, nargs='*')
        parser.add_argument('-t', '--to_brokers', help="List of Broker IDs to move partitions to (defaults to whole cluster)",
                            required=False, type=int, nargs='*')

    def process_cluster(self):
        # Make a deque for the target brokers so we can round-robin assignments
        todeque = deque(self.to_brokers)

        for broker_id in self.brokers:
            broker = self.cluster.brokers[broker_id]
            for position in broker.partitions:
                iterlist = list(broker.partitions[position])
                for partition in iterlist:
                    # Find a new replica for this partition
                    newreplica = None
                    attempts = 0
                    while attempts < len(todeque):
                        proposed = todeque.popleft()
                        todeque.append(proposed)
                        proposed_broker = self.cluster.brokers[proposed]
                        if proposed_broker not in partition.replicas:
                            newreplica = proposed_broker
                            break
                        attempts += 1

                    if newreplica is None:
                        raise NotEnoughReplicasException("Cannot find a new broker for {0}:{1} with replica list {2}".format(partition.topic.name,
                                                                                                                             partition.num,
                                                                                                                             partition.replicas))

                    # Replace the broker coming out with the new one
                    partition.swap_replicas(broker, newreplica)
