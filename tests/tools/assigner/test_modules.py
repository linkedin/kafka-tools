import unittest

import tests.tools.assigner.fixturemodules
from kafka.tools.assigner.modules import get_modules
from tests.tools.assigner.fixturemodules.inherited import ModuleInheritedFromBase


class ModulesTests(unittest.TestCase):
    def test_get_modules(self):
        module_list = get_modules(tests.tools.assigner.fixturemodules, tests.tools.assigner.fixturemodules.ModuleTesterBaseClass)
        assert module_list == [ModuleInheritedFromBase]
