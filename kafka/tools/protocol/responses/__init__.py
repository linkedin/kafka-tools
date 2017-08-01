import abc
import six

from kafka.tools.protocol.types.sequences import Sequence


@six.add_metaclass(abc.ABCMeta)
class BaseResponse():  # pragma: no cover
    @abc.abstractproperty
    def schema(self):
        pass

    @classmethod
    def from_dict(cls, value_dict):
        return cls(Sequence(value_dict, cls.schema))

    @classmethod
    def from_bytes(cls, correlation_id, byte_array):
        seq_obj, byte_array = Sequence.decode(byte_array, schema=cls.schema)
        rv = cls(seq_obj)
        rv.correlation_id = correlation_id
        return rv

    def __init__(self, sequence_obj):
        self._response = sequence_obj

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
