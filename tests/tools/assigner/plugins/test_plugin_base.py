import unittest

import argparse

from kafka.tools.assigner.models.cluster import Cluster
from kafka.tools.assigner.plugins import PluginModule


class PluginBaseTests(unittest.TestCase):
    def setUp(self):
        self.aparser = argparse.ArgumentParser(prog='kafka-assigner', description='Rejigger Kafka cluster partitions')
        self.subparsers = self.aparser.add_subparsers(help='Select manipulation module to use')
        self.args = argparse.Namespace()

        self.cluster = Cluster()
        self.plugin = PluginModule()

    def test_plugin_create(self):
        assert isinstance(self.plugin, PluginModule)

    def test_plugin_set_default_arguments(self):
        self.plugin.set_default_arguments(self.aparser)

    def test_plugin_set_arguments(self):
        self.plugin.set_arguments(self.args)

    def test_plugin_set_cluster(self):
        self.plugin.set_cluster(self.cluster)

    def test_plugin_after_sizes(self):
        self.plugin.after_sizes()

    def test_plugin_set_new_cluster(self):
        self.plugin.set_new_cluster(self.cluster)

    def test_plugin_set_batches(self):
        self.plugin.set_batches([])

    def test_plugin_before_execute_batch(self):
        self.plugin.before_execute_batch(1)

    def test_plugin_after_execute_batch(self):
        self.plugin.after_execute_batch(1)

    def test_plugin_before_ple(self):
        self.plugin.before_ple()

    def test_plugin_finished(self):
        self.plugin.finished()
