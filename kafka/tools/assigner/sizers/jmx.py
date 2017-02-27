from kafka.tools.assigner import log
from kafka.tools.assigner.exceptions import UnknownBrokerException
from kafka.tools.assigner.sizers import SizerModule

import jpype


class SizerJMX(SizerModule):
    name = 'jmx'
    helpstr = 'Get partition sizes by connection to each broker via JMX'

    def __init__(self, args, cluster, java_provider=None):
        super(SizerJMX, self).__init__(args, cluster)

        if java_provider is None:
            self._java_provider = jpype
            if 'libjvm' in self.properties:
                self._java_provider.startJVM(self.properties['libjvm'])
            else:
                self._java_provider.startJVM("/export/apps/jdk/JDK-1_8_0_72/jre/lib/amd64/server/libjvm.so")
        else:
            self._java_provider = java_provider

    def _fetch_bean(self, connection, bean):
        topic = bean.getKeyProperty("topic")
        partition = int(bean.getKeyProperty("partition"))
        size_bytes = connection.getAttribute(bean, "Value").value
        self.cluster.topics[topic].partitions[partition].set_size(size_bytes)

    def get_partition_sizes(self):
        # Get broker partition sizes
        for broker_id, broker in self.cluster.brokers.items():
            _validate_broker(broker)

            log.info("Getting partition sizes via JMX for {0}".format(broker.hostname))
            jmxurl = self._java_provider.javax.management.remote.JMXServiceURL(
                "service:jmx:rmi:///jndi/rmi://{0}:{1}/jmxrmi".format(broker.hostname, broker.jmx_port))
            jmxsoc = self._java_provider.javax.management.remote.JMXConnectorFactory.connect(jmxurl)

            connection = jmxsoc.getMBeanServerConnection()
            beans = connection.queryNames(self._java_provider.javax.management.ObjectName("kafka.log:name=Size,*"), None)
            for bean in beans:
                self._fetch_bean(connection, bean)

            jmxsoc.close()


def _validate_broker(broker):
    if broker.hostname is None:
        raise UnknownBrokerException("Cannot get sizes for broker ID {0} which has no hostname. "
                                     "Remove the broker from the cluster before balance".format(broker.id))
    if broker.jmx_port <= 0:
        raise UnknownBrokerException("Broker ID {0} does not have a JMX port configured".format(broker.id))
