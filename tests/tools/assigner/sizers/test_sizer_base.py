import unittest

from argparse import Namespace

from kafka.tools.assigner.models.cluster import Cluster
from kafka.tools.assigner.sizers import SizerModule


class SizerBaseTests(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster()
        self.args = Namespace()

    def test_sizer_create(self):
        sizer = SizerModule(self.args, self.cluster)
        assert isinstance(sizer, SizerModule)

    def test_get_partition_sizes(self):
        sizer = SizerModule(self.args, self.cluster)
        sizer.get_partition_sizes()
