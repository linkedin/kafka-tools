import abc
import collections
import six

from kafka.tools.decoding import decode_boolean, decode_int8, decode_int16, decode_int32, decode_int64, decode_string, decode_bytes


# Provide a mapping of type names to decoders
basic_type_decoder = {
    'boolean': decode_boolean,
    'int8': decode_int8,
    'int16': decode_int16,
    'int32': decode_int32,
    'int64': decode_int64,
    'string': decode_string,
    'bytes': decode_bytes,
}


@six.add_metaclass(abc.ABCMeta)
class BaseResponse():  # pragma: no cover
    @abc.abstractproperty
    def response_format(self):
        pass

    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
        self.response = []

    def __hash__(self):
        return id(self)

    def decode(self, byte_array):
        obj, byte_array = decode_struct(byte_array, {'type': self.response_format})
        self.response = obj

    @abc.abstractmethod
    def __str__(self):
        pass


def decode_object(byte_array, obj_def):
    if isinstance(obj_def['type'], six.string_types):
        return decode_basic_types(byte_array, obj_def)
    elif isinstance(obj_def['type'], collections.Sequence):
        return decode_struct(byte_array, obj_def)
    else:
        raise TypeError("Response definition type must be a string or a sequence, not: ".format(obj_def['type']))


def decode_basic_types(byte_array, obj_def):
    if obj_def['type'] == 'array':
        return decode_array(byte_array, obj_def)
    if obj_def['type'] in basic_type_decoder:
        return basic_type_decoder[obj_def['type']](byte_array)
    else:
        raise ValueError("Unknown protocol type: {0}".format(obj_def['type']))


def decode_struct(byte_array, obj_def):
    obj = []
    for struct_def in obj_def['type']:
        item, byte_array = decode_object(byte_array, struct_def)
        obj.append(item)
    return obj, byte_array


def decode_array(byte_array, obj_def):
    if len(byte_array) < 4:
        raise ValueError('Expected at least 4 bytes, only got {0}'.format(len(byte_array)))
    array_len, byte_array = decode_int32(byte_array)

    obj = []
    for i in range(array_len):
        item, byte_array = decode_object(byte_array, {'type': obj_def['item_type']})
        obj.append(item)
    return obj, byte_array
