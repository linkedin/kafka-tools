import unittest

import tests.tools.fixturemodules
import tests.tools.fixturemodules.noclass
from kafka.tools.modules import get_modules, is_class, check_class, check_classes_in_module
from tests.tools.fixturemodules.inherited import ModuleInheritedFromBase


class TestClassNoParent:
    pass


class ModulesTests(unittest.TestCase):
    def test_is_class(self):
        assert is_class(ModulesTests) is True

    def test_is_class_none(self):
        assert is_class(None) is False

    def test_is_class_notclass(self):
        just_a_var = 'foo'
        assert is_class(just_a_var) is False

    def test_check_class(self):
        assert check_class(ModulesTests, unittest.TestCase) == ModulesTests

    def test_check_class_noclass(self):
        just_a_var = 'foo'
        assert check_class(just_a_var, unittest.TestCase) is None

    def test_check_class_noparent(self):
        assert check_class(TestClassNoParent, tests.tools.fixturemodules.ModuleTesterBaseClass) is None

    def test_check_classes_in_module(self):
        module_list = check_classes_in_module(tests.tools.fixturemodules.inherited, tests.tools.fixturemodules.ModuleTesterBaseClass)
        assert module_list == [ModuleInheritedFromBase]

    def test_check_classes_in_module_noclasses(self):
        module_list = check_classes_in_module(tests.tools.fixturemodules.noclass, tests.tools.fixturemodules.ModuleTesterBaseClass)
        assert module_list == []

    def test_get_modules(self):
        module_list = get_modules(tests.tools.fixturemodules, tests.tools.fixturemodules.ModuleTesterBaseClass)
        assert module_list == [ModuleInheritedFromBase]
