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


class BaseResponse(object):
    equality_attrs = []
    response_format = None

    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
        self.response = []

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError
        return not any([getattr(self, attr_name) != getattr(other, attr_name) for attr_name in self.equality_attrs])

    def __hash__(self):
        return id(self)

    def decode(self, byte_array):
        if self.response_format is None:
            raise Exception("You cannot decode using BaseResponse")

        obj, byte_array = decode_struct(byte_array, {'type': self.response_format})
        self.response = obj

    def __str__(self):
        raise Exception("You cannot display a BaseResponse")


def decode_object(byte_array, obj_def):
    if isinstance(obj_def['type'], six.string_types):
        return decode_basic_types(byte_array, obj_def)
    elif isinstance(obj_def['type'], collections.Sequence):
        return decode_struct(byte_array, obj_def)
    else:
        raise Exception("Response definition type must be a string or a sequence, not: ".format(obj_def['type']))


def decode_basic_types(byte_array, obj_def):
    if obj_def['type'] == 'array':
        return decode_array(byte_array, obj_def)
    if obj_def['type'] in basic_type_decoder:
        return basic_type_decoder[obj_def['type']](byte_array)
    else:
        raise Exception("Unknown protocol type: {0}".format(obj_def['type']))


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
