class BaseModel(object):
    equality_attrs = []

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError
        return not any([getattr(self, attr_name) != getattr(other, attr_name) for attr_name in self.equality_attrs])
