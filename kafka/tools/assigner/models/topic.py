from kafka.tools.assigner.models.partition import Partition


class Topic:
    def __init__(self, name, partitions):
        self.name = name
        self.partitions = []
        self.cluster = None

        for i in range(partitions):
            self.add_partition(Partition(self, i))

    def __eq__(self, other):
        if not isinstance(other, Topic):
            raise TypeError
        return self.name == other.name

    # Shallow copy - do not copy partitions (zero partitions)
    def copy(self):
        newtopic = Topic(self.name, 0)
        newtopic.cluster = self.cluster
        return newtopic

    def add_partition(self, partition):
        self.partitions.append(partition)
