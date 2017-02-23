from kafka.tools.assigner import log
from kafka.tools.assigner.exceptions import UnknownBrokerException
from kafka.tools.assigner.sizers import SizerModule

import jpype
from jpype import javax


class SizerJMX(SizerModule):
    name = 'jmx'
    helpstr = 'Get partition sizes by connection to each broker via JMX'

    def __init__(self, args, cluster):
        super(SizerJMX, self).__init__(args, cluster)

        if 'libjvm' in self.properties:
            jpype.startJVM(self.properties['libjvm'])
        else:
            jpype.startJVM("/export/apps/jdk/JDK-1_8_0_72/jre/lib/amd64/server/libjvm.so")

    def get_partition_sizes(self):
        # Get broker partition sizes
        for broker_id, broker in self.cluster.brokers.items():
            if broker.hostname is None:
                raise UnknownBrokerException("Cannot get sizes for broker ID {0} which has no hostname. "
                                             "Remove the broker from the cluster before balance".format(broker_id))
            if broker.jmx_port <= 0:
                raise UnknownBrokerException("Broker ID {0} does not have a JMX port configured".format(broker_id))

            log.info("Getting partition sizes via JMX for {0}".format(broker.hostname))
            jmxurl = javax.management.remote.JMXServiceURL("service:jmx:rmi:///jndi/rmi://{0}:{1}/jmxrmi".format(broker.hostname, broker.jmx_port))
            jmxsoc = javax.management.remote.JMXConnectorFactory.connect(jmxurl)

            connection = jmxsoc.getMBeanServerConnection()
            beans = connection.queryNames(javax.management.ObjectName("kafka.log:name=Size,*"), None)
            for bean in beans:
                topic = bean.getKeyProperty("topic")
                partition = int(bean.getKeyProperty("partition"))
                size_bytes = connection.getAttribute(bean, "Value").value
                self.cluster.topics[topic].partitions[partition].set_size(size_bytes)

            jmxsoc.close()
