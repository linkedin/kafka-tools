import os
import unittest

from mock import patch

from kafka.tools.utilities import is_exec_file, get_tools_path, check_java_home, find_path_containing, json_loads
from kafka.tools.exceptions import ConfigurationException


class ToolsTests(unittest.TestCase):
    @patch('kafka.tools.utilities.os.path.isfile')
    @patch('kafka.tools.utilities.os.access')
    def test_is_exec_file(self, mock_access, mock_isfile):
        mock_access.return_value = True
        mock_isfile.return_value = True
        fname = 'somefile'
        res = is_exec_file(fname)
        assert res is True

    @patch('kafka.tools.utilities.os.path.isfile')
    @patch('kafka.tools.utilities.os.access')
    def test_is_exec_file_nonexistent(self, mock_access, mock_isfile):
        mock_access.return_value = True
        mock_isfile.return_value = False
        fname = 'this_file_does_not_exist_ksdjkfsj'
        res = is_exec_file(fname)
        assert res is False

    @patch('kafka.tools.utilities.os.path.isfile')
    @patch('kafka.tools.utilities.os.access')
    def test_is_exec_file_noexec(self, mock_access, mock_isfile):
        mock_access.return_value = False
        mock_isfile.return_value = True
        fname = 'this_file_does_not_exist_ksdjkfsj'
        res = is_exec_file(fname)
        assert res is False

    @patch('kafka.tools.utilities.find_path_containing')
    def test_get_tools_path_default(self, mock_find):
        mock_find.return_value = '/path/to/tools'
        tools_path = get_tools_path()
        assert tools_path == '/path/to/tools'

    @patch('kafka.tools.utilities.is_exec_file')
    def test_find_path_containing_found(self, mock_is_exec_file):
        mock_is_exec_file.return_value = True
        tools_path = find_path_containing('some_filename')
        assert tools_path is not None

    @patch('kafka.tools.utilities.is_exec_file')
    def test_find_path_containing_notfound(self, mock_is_exec_file):
        mock_is_exec_file.return_value = False
        self.assertRaises(ConfigurationException, find_path_containing, 'some_filename')

    @patch('kafka.tools.utilities.is_exec_file')
    def test_get_tools_path_explicit_found(self, mock_is_exec_file):
        mock_is_exec_file.return_value = True
        tools_path = get_tools_path('/path/to/file')
        assert tools_path is '/path/to/file'

    @patch('kafka.tools.utilities.is_exec_file')
    def test_get_tools_path_explicit_notfound(self, mock_is_exec_file):
        mock_is_exec_file.return_value = False
        self.assertRaises(ConfigurationException, get_tools_path, '/path/to/file')

    @patch('kafka.tools.utilities.is_exec_file')
    def test_check_java_home_found(self, mock_is_exec_file):
        os.environ['JAVA_HOME'] = '/path/to/java'
        mock_is_exec_file.return_value = True
        check_java_home()
        mock_is_exec_file.assert_called_with('/path/to/java/bin/java')

    @patch('kafka.tools.utilities.is_exec_file')
    def test_check_java_home_notfound(self, mock_is_exec_file):
        os.environ['JAVA_HOME'] = '/path/to/java'
        mock_is_exec_file.return_value = False
        self.assertRaises(ConfigurationException, check_java_home)

    def test_check_java_home_notset(self):
        del os.environ['JAVA_HOME']
        self.assertRaises(ConfigurationException, check_java_home)

    def test_json_loads(self):
        assert json_loads('1') == 1

    def test_json_loads_bytes(self):
        assert json_loads(b'1') == 1
