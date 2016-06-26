class Broker:
  def __init__(self, id, hostname):
    self.id = id
    self.hostname = None
    self.jmx_port = -1
    self.port = None
    self.version = None
    self.endpoints = None
    self.timestamp = None
    self.cluster = None
    self.partitions = {}

  @classmethod
  def create_from_json(cls, broker_id, jsondata):
    data = json.loads(jsondata)
    newbroker = cls(broker_id)

    # These things are required, and we can't proceed if they're not there
    try:
      newbroker.hostname = data['host']
    except KeyError:
      raise ConfigurationException("Cannot parse broker data in zookeeper. This version of Kafka may not be supported.")

    # These things are optional, and are pulled in for convenience or extra features
    try:
      newbroker.jmx_port = data['jmx_port']
      newbroker.port = data['port']
      newbroker.version = data['version']
      newbroker.endpoints = data['endpoints']
      newbroker.timestamp = data['timestamp']
    except KeyError:
      pass

    return newbroker

  # Shallow copy - do not copy partitions map over
  def copy(self):
    newbroker = Broker(self.id)
    newbroker.hostname = self.hostname
    newbroker.jmx_port = self.jmx_port
    newbroker.port = self.port
    newbroker.version = self.version
    newbroker.endpoints = self.endpoints
    newbroker.timestamp = self.timestamp
    newbroker.cluster = self.cluster
    return newbroker

  def num_leaders(self):
    if 0 in self.partitions:
      return len(self.partitions[0])
    else:
      return 0

  def percent_leaders(self):
    if self.num_partitions() == 0:
      return 0.0
    return (self.num_leaders() / self.num_partitions()) * 100

  def total_size(self):
    return sum([p.size for pos in self.partitions for p in self.partitions[pos]], 0)

  def num_partitions(self):
    return sum([len(self.partitions[pos]) for pos in self.partitions], 0)
