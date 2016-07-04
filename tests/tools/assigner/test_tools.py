import os
import unittest

from mock import patch

from kafka.tools.assigner.tools import is_exec_file, get_tools_path, check_java_home
from kafka.tools.assigner.exceptions import ConfigurationException


class ToolsTests(unittest.TestCase):
    @patch('kafka.tools.assigner.tools.os.path.isfile')
    @patch('kafka.tools.assigner.tools.os.access')
    def test_is_exec_file(self, mock_access, mock_isfile):
        mock_access.return_value = True
        mock_isfile.return_value = True
        fname = 'somefile'
        res = is_exec_file(fname)
        assert res is True

    @patch('kafka.tools.assigner.tools.os.path.isfile')
    @patch('kafka.tools.assigner.tools.os.access')
    def test_is_exec_file_nonexistent(self, mock_access, mock_isfile):
        mock_access.return_value = True
        mock_isfile.return_value = False
        fname = 'this_file_does_not_exist_ksdjkfsj'
        res = is_exec_file(fname)
        assert res is False

    @patch('kafka.tools.assigner.tools.os.path.isfile')
    @patch('kafka.tools.assigner.tools.os.access')
    def test_is_exec_file_noexec(self, mock_access, mock_isfile):
        mock_access.return_value = False
        mock_isfile.return_value = True
        fname = 'this_file_does_not_exist_ksdjkfsj'
        res = is_exec_file(fname)
        assert res is False

    @patch('kafka.tools.assigner.tools.is_exec_file')
    def test_get_tools_path_default_found(self, mock_is_exec_file):
        mock_is_exec_file.return_value = True
        tools_path = get_tools_path()
        assert tools_path is not None

    @patch('kafka.tools.assigner.tools.is_exec_file')
    def test_get_tools_path_default_notfound(self, mock_is_exec_file):
        mock_is_exec_file.return_value = False
        self.assertRaises(ConfigurationException, get_tools_path)

    @patch('kafka.tools.assigner.tools.is_exec_file')
    def test_get_tools_path_explicit_found(self, mock_is_exec_file):
        mock_is_exec_file.return_value = True
        tools_path = get_tools_path('/path/to/file')
        assert tools_path is '/path/to/file'

    @patch('kafka.tools.assigner.tools.is_exec_file')
    def test_get_tools_path_explicit_notfound(self, mock_is_exec_file):
        mock_is_exec_file.return_value = False
        self.assertRaises(ConfigurationException, get_tools_path, '/path/to/file')

    @patch('kafka.tools.assigner.tools.is_exec_file')
    def test_check_java_home_found(self, mock_is_exec_file):
        os.environ['JAVA_HOME'] = '/path/to/java'
        mock_is_exec_file.return_value = True
        check_java_home()
        mock_is_exec_file.assert_called_with('/path/to/java/bin/java')

    @patch('kafka.tools.assigner.tools.is_exec_file')
    def test_check_java_home_notfound(self, mock_is_exec_file):
        os.environ['JAVA_HOME'] = '/path/to/java'
        mock_is_exec_file.return_value = False
        self.assertRaises(ConfigurationException, check_java_home)

    def test_check_java_home_notset(self):
        del os.environ['JAVA_HOME']
        self.assertRaises(ConfigurationException, check_java_home)
