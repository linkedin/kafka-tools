import unittest
from mock import patch
from tests.tools.protocol.utilities import validate_schema

from kafka.tools.protocol.requests.update_metadata_v0 import UpdateMetadataV0Request


class UpdateMetadataV0Tests(unittest.TestCase):
    @patch('kafka.tools.protocol.requests.update_metadata_v0._process_arguments')
    def test_process_arguments_class(self, mock_process):
        mock_process.return_value = 'fake_return_value'
        val = UpdateMetadataV0Request.process_arguments(['fake_arguments'])
        mock_process.assert_called_once_with("UpdateMetadataV0", ['fake_arguments'])
        assert val == 'fake_return_value'

    def test_schema(self):
        validate_schema(UpdateMetadataV0Request.schema)
