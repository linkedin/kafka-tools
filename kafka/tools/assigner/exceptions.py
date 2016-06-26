class AssignerException(Exception):
  errstr = "Unknown Assigner Exception"

  def __init__(self, custom_errstr=None):
    super(AssignerException, self).__init__()
    if str is not None:
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
