import unittest

import kafka.tools.assigner.exceptions


class ExceptionTests(unittest.TestCase):
    def test_exception_assigner_plain(self):
        e = kafka.tools.assigner.exceptions.AssignerException()
        assert str(e) == "Unknown Assigner Exception"
        assert isinstance(e, Exception)

    def test_exception_assigner_custom(self):
        e = kafka.tools.assigner.exceptions.AssignerException("custom text")
        assert str(e) == "custom text"

    def test_exception_notfound(self):
        e = kafka.tools.assigner.exceptions.ReplicaNotFoundException()
        assert isinstance(e, kafka.tools.assigner.exceptions.AssignerException)

    def test_exception_notenough(self):
        e = kafka.tools.assigner.exceptions.NotEnoughReplicasException()
        assert isinstance(e, kafka.tools.assigner.exceptions.AssignerException)

    def test_exception_config(self):
        e = kafka.tools.assigner.exceptions.ConfigurationException()
        assert isinstance(e, kafka.tools.assigner.exceptions.AssignerException)

    def test_exception_zookeeper(self):
        e = kafka.tools.assigner.exceptions.ZookeeperException()
        assert isinstance(e, kafka.tools.assigner.exceptions.AssignerException)

    def test_exception_consistency(self):
        e = kafka.tools.assigner.exceptions.ClusterConsistencyException()
        assert isinstance(e, kafka.tools.assigner.exceptions.AssignerException)

    def test_exception_programming(self):
        e = kafka.tools.assigner.exceptions.ProgrammingException()
        assert isinstance(e, kafka.tools.assigner.exceptions.AssignerException)

    def test_exception_reassignment_failed(self):
        e = kafka.tools.assigner.exceptions.ReassignmentFailedException()
        assert isinstance(e, kafka.tools.assigner.exceptions.AssignerException)

    def test_exception_unknown_broker(self):
        e = kafka.tools.assigner.exceptions.UnknownBrokerException()
        assert isinstance(e, kafka.tools.assigner.exceptions.AssignerException)
