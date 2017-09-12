import abc
import pprint
import six


def _decode_plain_type(value_type, buf):
    if value_type == 'int8':
        return buf.getInt8()
    elif value_type == 'int16':
        return buf.getInt16()
    elif value_type == 'int32':
        return buf.getInt32()
    elif value_type == 'int64':
        return buf.getInt64()
    elif value_type == 'string':
        val_len = buf.getInt16()
        return None if val_len == -1 else buf.get(val_len).decode("utf-8")
    elif value_type == 'bytes':
        val_len = buf.getInt32()
        return None if val_len == -1 else buf.get(val_len)
    elif value_type == 'boolean':
        return buf.getInt8() == 1
    else:
        raise NotImplementedError("Reference to non-implemented type in schema: {0}".format(value_type))


def _decode_array(array_schema, buf):
    array_len = buf.getInt32()
    if array_len == -1:
        return None

    if isinstance(array_schema, six.string_types):
        return [_decode_plain_type(array_schema, buf) for i in range(array_len)]
    else:
        return [_decode_sequence(array_schema, buf) for i in range(array_len)]


def _decode_sequence(sequence_schema, buf):
    val = {}
    for entry in sequence_schema:
        if entry['type'].lower() == 'array':
            val[entry['name']] = _decode_array(entry['item_type'], buf)
        else:
            val[entry['name']] = _decode_plain_type(entry['type'].lower(), buf)
    return val


@six.add_metaclass(abc.ABCMeta)
class BaseResponse():  # pragma: no cover
    @abc.abstractproperty
    def schema(self):
        pass

    @classmethod
    def from_bytebuffer(cls, correlation_id, buf):
        seq_obj = _decode_sequence(cls.schema, buf)
        rv = cls(seq_obj)
        rv.correlation_id = correlation_id
        return rv

    def __init__(self, sequence_obj):
        self._response = sequence_obj

    def __hash__(self):
        return id(self)

    def __str__(self):
        pp = pprint.PrettyPrinter(indent=4)
        return pp.pformat(self._response)

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
