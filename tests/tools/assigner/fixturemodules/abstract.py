import abc
from tests.tools.assigner.fixturemodules import ModuleTesterBaseClass


class ModuleAbstract(ModuleTesterBaseClass):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def absprop(self):
        pass
