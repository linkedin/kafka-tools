from kafka.tools.assigner.exceptions import ReplicaNotFoundException


class Partition:
    def __init__(self, topic, num):
        self.topic = topic
        self.num = num
        self.replicas = []
        self.size = 0

    def __eq__(self, other):
        if not isinstance(other, Partition):
            raise TypeError
        return (self.topic == other.topic) and (self.num == other.num)

    # Shallow copy - do not copy replica list (zero length)
    def copy(self):
        newpartition = Partition(self.topic, self.num)
        newpartition.size = self.size
        return newpartition

    # Set the current size of this partition iff it is larger than the currently known size
    def set_size(self, size):
        if size > self.size:
            self.size = size

    def dict_for_reassignment(self):
        return {"topic": self.topic.name, "partition": self.num, "replicas": [broker.id for broker in self.replicas]}

    def dict_for_replica_election(self):
        return {"topic": self.topic.name, "partition": self.num}

    # Given a broker, add it to this partition as a replica
    # If position is not specified, default to the end of the replica list
    def add_replica(self, broker, position=-1):
        if position < 0:
            position = len(self.replicas)
        self._add_broker_partition(position, broker)
        self.replicas.insert(position, broker)

    # Remove the specified broker from the replica list of this partition
    # If the replica does not exist on this partition, throw an exception
    def remove_replica(self, broker):
        try:
            position = self.replicas.index(broker)
        except ValueError:
            raise ReplicaNotFoundException

        broker.partitions[position].remove(self)
        self.replicas.remove(broker)

    # Remove one broker from the replica list and replace it at the same position with another
    # This just calls add_replica and remove_replica, but we do it a lot
    def swap_replicas(self, remove_broker, add_broker):
        try:
            position = self.replicas.index(remove_broker)
        except ValueError:
            raise ReplicaNotFoundException

        self.remove_replica(remove_broker)
        self.add_replica(add_broker, position)

    # Given two brokers that appear in the replica list, swap their positions (making sure to adjust the broker objects as well)
    # If either replica is not in the list, throw an exception
    def swap_replica_positions(self, broker1, broker2):
        try:
            p1 = self.replicas.index(broker1)
            p2 = self.replicas.index(broker2)
        except ValueError:
            raise ReplicaNotFoundException

        # First, change the position of this partition on the first broker object to p2
        self._remove_broker_partition(broker1)
        self._add_broker_partition(p2, broker1)

        # Then change the position of this partition on the second broker object to p1
        self._remove_broker_partition(broker2)
        self._add_broker_partition(p1, broker2)

        # Last, swap the replica positons on this partition object
        self.replicas[p1] = broker2
        self.replicas[p2] = broker1

    # Helper function to add a partition to a broker
    # This should never be called - please use the add_replica method
    def _add_broker_partition(self, pos, broker):
        if pos not in broker.partitions:
            broker.partitions[pos] = [self]
        else:
            broker.partitions[pos].append(self)

    # Helper function to remove a partition from a broker
    # This should never be called - please use the remove_replica method
    def _remove_broker_partition(self, broker):
        pos = self.replicas.index(broker)
        broker.partitions[pos].remove(self)
