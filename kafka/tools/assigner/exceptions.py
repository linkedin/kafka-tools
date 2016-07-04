class AssignerException(Exception):
    errstr = "Unknown Assigner Exception"

    def __init__(self, custom_errstr=None):
        super(AssignerException, self).__init__()
        if custom_errstr is not None:
            self.errstr = custom_errstr

    def __str__(self):
        return self.errstr


class ReplicaNotFoundException(AssignerException):
    errstr = "The specified replica is not present in the partition"


class NotEnoughReplicasException(AssignerException):
    errstr = "There were not enough replicas left for a partition"


class ConfigurationException(AssignerException):
    errstr = "There was an error in the configuration provided"


class ZookeeperException(AssignerException):
    errstr = "There was an error connecting to Zookeeper"


class ClusterConsistencyException(AssignerException):
    errstr = "There is a problem with the consistency of the cluster topics and/or partitions"


class ProgrammingException(AssignerException):
    errstr = "There is an error in the structure of the code"


class ReassignmentFailedException(AssignerException):
    errstr = "The partition reassignment failed"


class UnknownBrokerException(AssignerException):
    errstr = "There is an unknown broker hostname"
