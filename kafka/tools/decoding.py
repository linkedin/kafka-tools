import binascii
import struct


def decode_boolean(byte_array):
    obj, byte_array = decode_int8(byte_array)
    return obj != 0, byte_array


def decode_int8(byte_array):
    if len(byte_array) < 1:
        raise ValueError('Expected 1 byte, only got {0}'.format(len(byte_array)))
    items = struct.unpack('>b', byte_array[0:1])
    return items[0], byte_array[1:]


def decode_int16(byte_array):
    if len(byte_array) < 2:
        raise ValueError('Expected 2 bytes, only got {0}'.format(len(byte_array)))
    items = struct.unpack('>h', byte_array[0:2])
    return items[0], byte_array[2:]


def decode_int32(byte_array):
    if len(byte_array) < 4:
        raise ValueError('Expected 4 bytes, only got {0}'.format(len(byte_array)))
    items = struct.unpack('>i', byte_array[0:4])
    return items[0], byte_array[4:]


def decode_int64(byte_array):
    if len(byte_array) < 8:
        raise ValueError('Expected 8 bytes, only got {0}'.format(len(byte_array)))
    items = struct.unpack('>q', byte_array[0:8])
    return items[0], byte_array[8:]


def decode_string(byte_array):
    if len(byte_array) < 2:
        raise ValueError('Expected at least 2 bytes, only got {0}'.format(len(byte_array)))
    str_len, str_data = decode_int16(byte_array)
    if str_len == -1:
        return None, str_data
    if str_len > len(str_data):
        raise ValueError('Expected {0} bytes, only got {1}'.format(str_len + 2, len(byte_array)))
    return str_data[0:str_len], str_data[str_len:]


def decode_bytes(byte_array):
    if len(byte_array) < 2:
        raise ValueError('Expected at least 2 bytes, only got {0}'.format(len(byte_array)))
    str_len, str_data = decode_int16(byte_array)
    if str_len == -1:
        return None, str_data
    if str_len > len(str_data):
        raise ValueError('Expected {0} bytes, only got {1}'.format(str_len + 2, len(byte_array)))
    return binascii.hexlify(str_data[0:str_len]), str_data[str_len:]
