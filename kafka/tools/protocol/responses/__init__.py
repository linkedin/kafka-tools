import abc
import six

from kafka.tools.protocol.types.sequences import Sequence


@six.add_metaclass(abc.ABCMeta)
class BaseResponse():  # pragma: no cover
    @abc.abstractproperty
    def schema(self):
        pass

    def __init__(self, correlation_id, byte_array):
        self.correlation_id = correlation_id

        obj, byte_array = Sequence.decode(byte_array, schema=self.schema)
        self._response = obj

    def __hash__(self):
        return id(self)

    def __str__(self):
        return str(self._response)

    def __len__(self):
        return len(self._response)

    def __contains__(self, k):
        return k in self._response

    def __getitem__(self, k):
        return self._response[k]

    def __setitem__(self, k):
        raise NotImplementedError

    def __delitem__(self, k):
        raise NotImplementedError
