import collections
import six


def _validate_schema_entry_type(entry_type):
    if isinstance(entry_type, six.string_types):
        if entry_type.lower() not in ('int8', 'int16', 'int32', 'int64', 'string', 'bytes', 'boolean', 'array'):
            raise TypeError('Unknown schema data type: {0}'.format(entry_type))
    elif isinstance(entry_type, collections.Sequence):
        validate_schema(entry_type)
    else:
        raise TypeError('Schema type must be a string type or a list, not {0}'.format(type(entry_type)))


def _validate_schema_entry(entry):
    if not isinstance(entry, dict):
        raise TypeError('Expected schema entries to be dicts, not {0}'.format(type(entry)))
    if not all(k in entry for k in ('name', 'type')):
        raise KeyError('Schema entries must have both name and type keys')
    if not isinstance(entry['name'], six.string_types):
        raise TypeError('Schema entry name must be a string, not {0}'.format(type(entry['name'])))


# A method for use in tests to validate that a schema (for requests or responses) is properly formed
def validate_schema(schema):
    for entry in schema:
        _validate_schema_entry(entry)
        if isinstance(entry['type'], six.string_types) and (entry['type'].lower() == 'array'):
            if 'item_type' not in entry:
                raise KeyError('Array schema entries must have an item_type key')
            _validate_schema_entry_type(entry['item_type'])
        else:
            _validate_schema_entry_type(entry['type'])
