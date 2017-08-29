import re
try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen

from kafka.tools import log
from kafka.tools.exceptions import UnknownBrokerException, ConfigurationException
from kafka.tools.assigner.sizers import SizerModule


class SizerPrometheus(SizerModule):
    name = 'prometheus'
    helpstr = 'Get partition sizes by connection to each broker via Prometheus metrics (https://prometheus.io/)'

    def __init__(self, args, cluster):
        super(SizerPrometheus, self).__init__(args, cluster)
        self._prom_line_re = re.compile(
            r'^(?P<name>[a-zA-Z0-9_]+){(?P<labels>[a-zA-Z0-9]+=\".*\")+(?:,+)?(?:\ +)?} (?P<value>[0-9.E]+)$')
        self._prom_label_re = re.compile(
            r'^(?P<name>[a-zA-Z0-9]+)=\"(?P<value>.*)\"$')

    def _validate_properties(self):
        for propname in ['size_metric_name', 'metrics_port']:
            if propname not in self.properties:
                raise ConfigurationException(
                    "Prometheus sizer requires '{}' property to be set".format(
                        propname))

    def _parse_prometheus_labels(self, label_parts):
        """
        Takes list of labels in Prometheus format and turns those into a dict.
        Example input:
        ['partition="3"', 'topic="mytopic"']
        Will return:
        {
            'partition: '3',
            'topic': 'mytopic'
        }

        :param label_parts: list of label strings
        :return: dict with parsed labels
        """
        labels = {}
        for label in label_parts:
            # be sure to strip spaces around the label
            lm = self._prom_label_re.match(label.strip())
            if lm and len(lm.groups()) == 2:
                labels[lm.group('name')] = lm.group('value')
        return labels

    def _parse_prometheus_value(self, value):
        try:
            val = float(value)
        except ValueError:
            raise UnknownBrokerException(
                "Prometheus sizer cannot convert '{}' to float".format(value))
        return int(val)

    def _parse_prometheus_metric(self, text):
        """
        Parse a metric string in Prometheus format, example:

        kafka_log_size{partition="1",topic="mytopic",} 2.759052553E9

        Return dict with name, labels and value, example:

        {
            'name': 'kafka_log_size',
            'labels': {'partition': '1', 'topic': 'mytopic'},
            'value': 2759052553
        }

        If name, labels or value is missing None will be returned.
        """
        metric = {}
        m = self._prom_line_re.match(text)
        if m and len(m.groups()) == 3:
            metric['name'] = m.group('name')
            metric['value'] = self._parse_prometheus_value(m.group('value'))
            metric['labels'] = self._parse_prometheus_labels(m.group('labels').split(','))

        return metric or None

    def _get_prometheus_metrics(self, hostname, port, path):
        url = 'http://{}:{}{}'.format(hostname, port, path)
        body = ''
        try:
            response = urlopen(url)
            if response.getcode() == 200:
                body = response.read()
            else:
                raise UnknownBrokerException(
                    "Prometheus sizer received invalid (!=200) response code from {}: {}".format(url, response.getcode()))
        except Exception as e:
            raise UnknownBrokerException("Prometheus sizer failed to collect metrics from {}. "
                                         "Error: {}".format(url, e))
        return body.splitlines()

    def _query_prometheus(self, hostname):
        size_metric_name = self.properties['size_metric_name']
        metrics_port = self.properties['metrics_port']
        metrics_path = self.properties.get('metrics_path', '/metrics')
        topic_label = self.properties.get('topic_label', 'topic')
        partition_label = self.properties.get('partition_label', 'partition')
        for text in self._get_prometheus_metrics(hostname, metrics_port, metrics_path):
            m = self._parse_prometheus_metric(text)
            if m and m['name'] == size_metric_name:
                topic = m['labels'][topic_label]
                try:
                    partition = int(m['labels'][partition_label])
                except ValueError:
                    raise UnknownBrokerException(
                        "Prometheus sizer failed to parse partition '{}' as int".format(m['labels'][partition_label]))
                self.cluster.topics[topic].partitions[partition].set_size(m['value'])

    def get_partition_sizes(self):
        self._validate_properties()
        for broker_id, broker in self.cluster.brokers.items():
            if broker.hostname is None:
                raise UnknownBrokerException("Cannot get sizes for broker ID {0} which has no hostname. "
                                             "Remove the broker from the cluster before balance".format(broker_id))

            log.info("Getting partition sizes via Prometheus exporter for {0}".format(broker.hostname))
            self._query_prometheus(broker.hostname)
